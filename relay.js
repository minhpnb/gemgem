/* =====================================================================
   relay.js — Lớp trung chuyển (relay) cho DuoGames qua Firebase
   Realtime Database, thay cho PeerJS/WebRTC.

   File này cung cấp một shim TƯƠNG THÍCH API PeerJS (constructor `Peer`,
   sự kiện 'open'/'connection'/'error', conn.on('open'/'data'/'close'/
   'error'), conn.send/close/open/peer/connectionId) để các trang game
   (chess/xiangqi/caro/ngua) giữ nguyên gần như toàn bộ code mạng.

   Cách hoạt động (mọi traffic đi qua HTTPS/WebSocket của Firebase):
     /rooms/<CODE>/host            — host "giữ chỗ" phòng (heartbeat 7s,
                                      onDisconnect tự xóa; claim cũ hơn
                                      ~20s coi như chết, host mới lấy lại)
     /rooms/<CODE>/conns/<connId>  — mỗi kết nối khách→host:
        { a: <id khách>, b: <id host>, ts }
        m/a/<pushId>  hàng đợi tin khách → host
        m/b/<pushId>  hàng đợi tin host → khách
        p/a, p/b      presence từng đầu (onDisconnect tự xóa → đầu kia
                      nhận 'close' khi một tab chết/đóng)

   CẤU HÌNH: điền firebaseConfig thật của project vào FIREBASE_CONFIG
   bên dưới (thay các placeholder __FIREBASE_*__). Nếu còn placeholder,
   Relay.configured === false và các trang sẽ báo "Chưa cấu hình
   Firebase" thay vì treo.
   ===================================================================== */
(function () {
  'use strict';

  /* === CẤU HÌNH FIREBASE — THAY CÁC PLACEHOLDER KHI DEPLOY === */
  var FIREBASE_CONFIG = {
    apiKey: "AIzaSyCrZQeb4mwK3AtCFuiaho7dV0mIVleyJss",
    authDomain: "gemgem-9c68e.firebaseapp.com",
    databaseURL: "https://gemgem-9c68e-default-rtdb.asia-southeast1.firebasedatabase.app",
    projectId: "gemgem-9c68e",
    appId: "1:452037023640:web:fedfdfdca9c1e800527ded"
  };
  /* Cho phép ghi đè khi kiểm thử (harness) — bản deploy không dùng. */
  if (typeof window !== 'undefined' && window.__FIREBASE_CONFIG__) {
    FIREBASE_CONFIG = window.__FIREBASE_CONFIG__;
  }

  var STALE_MS = 20000;      // claim/heartbeat cũ hơn 20s coi như đã chết
  var HEARTBEAT_MS = 7000;   // host làm mới claim mỗi 7s

  function configIsPlaceholder() {
    for (var k in FIREBASE_CONFIG) {
      var v = FIREBASE_CONFIG[k];
      if (typeof v === 'string' && v.indexOf('__FIREBASE_') === 0) return true;
    }
    return false;
  }

  var sdkMissing = (typeof firebase === 'undefined') ||
    typeof firebase.initializeApp !== 'function' ||
    typeof firebase.database !== 'function';

  var Relay = {
    configured: !sdkMissing && !configIsPlaceholder(),
    error: null,
    version: 'firebase-relay-1'
  };
  if (sdkMissing) Relay.error = 'sdk-missing';
  else if (!Relay.configured) Relay.error = 'not-configured';
  if (typeof window !== 'undefined') window.Relay = Relay;

  /* ---------------- Tiện ích ---------------- */
  function randStr(n) {
    var chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    var s = '';
    for (var i = 0; i < n; i++) s += chars.charAt(Math.floor(Math.random() * chars.length));
    return s;
  }
  function roomCodeOf(peerId) {
    if (typeof peerId === 'string' && peerId.indexOf('duogames-') === 0) return peerId.slice('duogames-'.length);
    return String(peerId || '');
  }
  function cloneJson(obj) {
    if (obj === undefined) return null;
    try { return JSON.parse(JSON.stringify(obj)); } catch (e) { return obj; }
  }
  function serverTs() { return firebase.database.ServerValue.TIMESTAMP; }

  /* Emitter tối giản. Riêng sự kiện 'open': nếu đã mở rồi mà handler mới
     được gắn (mẫu sendTo chờ-open của các game), gọi lại handler bất đồng
     bộ để không mất tin bắt tay. */
  function Emitter() { this._ev = {}; }
  Emitter.prototype.on = function (evt, cb) {
    if (typeof cb !== 'function') return this;
    (this._ev[evt] = this._ev[evt] || []).push(cb);
    if (evt === 'open' && this.open === true) {
      var self = this;
      setTimeout(function () { if (self.open) cb.call(self); }, 0);
    }
    return this;
  };
  Emitter.prototype._emit = function (evt) {
    var list = this._ev[evt];
    if (!list || !list.length) return;
    var args = Array.prototype.slice.call(arguments, 1);
    var self = this;
    list.slice().forEach(function (cb) {
      try { cb.apply(self, args); } catch (e) { /* handler của game tự lo */ }
    });
  };

  /* =====================================================================
     Nếu chưa cấu hình / thiếu SDK: vẫn định nghĩa Peer dạng "stub" để
     mọi đường code lỡ tạo Peer đều nhận lỗi rõ ràng thay vì treo.
     ===================================================================== */
  if (!Relay.configured) {
    var StubPeer = function () {
      Emitter.call(this);
      this.id = null;
      this.destroyed = false;
      this.open = false;
      var self = this;
      setTimeout(function () {
        self._emit('error', { type: Relay.error === 'sdk-missing' ? 'sdk-unavailable' : 'firebase-not-configured' });
      }, 0);
    };
    StubPeer.prototype = Object.create(Emitter.prototype);
    StubPeer.prototype.connect = function () {
      var c = new Emitter();
      c.open = false; c.peer = null; c.connectionId = null;
      c.send = function () { throw new Error('relay-not-configured'); };
      c.close = function () {};
      return c;
    };
    StubPeer.prototype.destroy = function () { this.destroyed = true; };
    if (typeof window !== 'undefined') window.Peer = StubPeer;
    return;
  }

  /* ---------------- Khởi tạo Firebase (1 lần / trang) ---------------- */
  var app = null;
  try {
    if (firebase.apps && firebase.apps.length) app = firebase.apps[0];
    else app = firebase.initializeApp(FIREBASE_CONFIG);
  } catch (e) {
    Relay.configured = false;
    Relay.error = 'init-failed';
    if (typeof window !== 'undefined') window.Relay = Relay;
    var BadPeer = function () {
      Emitter.call(this);
      this.destroyed = false; this.open = false;
      var self = this;
      setTimeout(function () { self._emit('error', { type: 'firebase-init-failed' }); }, 0);
    };
    BadPeer.prototype = Object.create(Emitter.prototype);
    BadPeer.prototype.connect = function () { throw new Error('firebase-init-failed'); };
    BadPeer.prototype.destroy = function () { this.destroyed = true; };
    if (typeof window !== 'undefined') window.Peer = BadPeer;
    return;
  }
  var db = firebase.database();

  function roomsRef(code) { return db.ref('rooms/' + code); }

  /* =====================================================================
     RelayConn — một đầu của kết nối khách↔host.
     side: 'a' = đầu khách (người tạo kết nối), 'b' = đầu host.
     ===================================================================== */
  function RelayConn(ownerPeer, code, connId, remoteId, side) {
    Emitter.call(this);
    this._owner = ownerPeer;
    this._code = code;
    this._side = side;
    this.peer = remoteId;               // id của đầu bên kia (giống PeerJS)
    this.connectionId = connId;         // id duy nhất của kết nối
    this.open = false;
    this._closed = false;
    this._closeFired = false;
    this._sendBuffer = [];
    this._sawRemotePresence = false;
    this._refs = [];
    /* Stub tương thích các hook ICE của game: presence của relay đóng vai
       trò tín hiệu sống/chết, nên iceConnectionState luôn 'connected' và
       addEventListener không bao giờ bắn 'failed'. */
    this.peerConnection = {
      iceConnectionState: 'connected',
      addEventListener: function () {},
      removeEventListener: function () {},
      close: function () {}
    };
  }
  RelayConn.prototype = Object.create(Emitter.prototype);

  RelayConn.prototype._nodeRef = function () {
    return roomsRef(this._code).child('conns/' + this.connectionId);
  };
  RelayConn.prototype._myPresenceRef = function () {
    return this._nodeRef().child('p/' + this._side);
  };
  RelayConn.prototype._remotePresenceRef = function () {
    return this._nodeRef().child('p/' + (this._side === 'a' ? 'b' : 'a'));
  };
  RelayConn.prototype._inQueueRef = function () {
    /* Hàng đợi ĐỌC = tin do đầu bên kia gửi: bên kia ở phía 'a' thì đọc m/a */
    return this._nodeRef().child('m/' + (this._side === 'a' ? 'b' : 'a'));
  };
  RelayConn.prototype._outQueueRef = function () {
    return this._nodeRef().child('m/' + this._side);
  };

  RelayConn.prototype._listen = function () {
    var self = this;
    /* Tin đến: child_added bắn theo thứ tự khóa push (đúng thứ tự gửi),
       kể cả loạt tin gửi dồn. Nhận xong thì xóa khỏi hàng đợi. */
    var inRef = this._inQueueRef();
    var onChild = inRef.on('child_added', function (snap) {
      if (self._closed) return;
      var val = snap.val();
      try { snap.ref.remove(); } catch (e) {}
      if (val !== null && val !== undefined) self._emit('data', val);
    });
    this._refs.push({ ref: inRef, evt: 'child_added', cb: onChild });
    /* Presence đầu bên kia biến mất (tab chết / đóng chủ động) → 'close' */
    var rpRef = this._remotePresenceRef();
    var onPresence = rpRef.on('value', function (snap) {
      if (self._closed) return;
      if (snap.exists()) {
        self._sawRemotePresence = true;
        if (self._side === 'a') self._markOpen(); // khách: host đã nhận → mở
      } else if (self._sawRemotePresence) {
        self._fireClose();
      }
    });
    this._refs.push({ ref: rpRef, evt: 'value', cb: onPresence });
    /* Node kết nối bị xóa hẳn (host hủy peer…) → cũng coi như đóng */
    var nodeRef = this._nodeRef();
    var onNode = nodeRef.on('value', function (snap) {
      if (self._closed) return;
      if (!snap.exists() && self._sawRemotePresence) self._fireClose();
    });
    this._refs.push({ ref: nodeRef, evt: 'value', cb: onNode });
  };

  RelayConn.prototype._markOpen = function () {
    if (this.open || this._closed) return;
    this.open = true;
    var buf = this._sendBuffer;
    this._sendBuffer = [];
    for (var i = 0; i < buf.length; i++) this._pushNow(buf[i]);
    this._emit('open');
  };

  RelayConn.prototype._pushNow = function (obj) {
    try { this._outQueueRef().push(cloneJson(obj)); } catch (e) { /* node đã bị dọn */ }
  };

  RelayConn.prototype.send = function (obj) {
    if (this._closed) throw new Error('Connection is closed');
    if (!this.open) { // đệm tới khi mở (PeerJS bản mới cũng xếp hàng chờ open)
      if (this._sendBuffer.length < 200) this._sendBuffer.push(obj);
      return;
    }
    this._pushNow(obj);
  };

  RelayConn.prototype._detach = function () {
    this._refs.forEach(function (r) {
      try { r.ref.off(r.evt, r.cb); } catch (e) {}
    });
    this._refs = [];
  };

  RelayConn.prototype._fireClose = function () {
    if (this._closeFired) return;
    this._closeFired = true;
    this._closed = true;
    this.open = false;
    this._detach();
    if (this._owner) this._owner._forgetConn(this);
    this._emit('close');
  };

  RelayConn.prototype.close = function () {
    if (this._closed) { this._fireClose(); return; }
    this._closed = true;
    this.open = false;
    var self = this;
    /* Gỡ presence của mình: đầu bên kia sẽ nhận 'close'. Nếu mình là bên
       tạo kết nối (khách) thì xóa luôn node kết nối cho sạch. */
    try { this._myPresenceRef().remove(); } catch (e) {}
    if (this._side === 'a') {
      try { this._nodeRef().remove(); } catch (e) {}
    }
    setTimeout(function () { self._fireClose(); }, 0);
  };

  /* Đăng presence của đầu mình + onDisconnect tự gỡ khi tab chết */
  RelayConn.prototype._announcePresence = function () {
    var p = this._myPresenceRef();
    try { p.onDisconnect().remove(); } catch (e) {}
    try { p.set({ ts: serverTs() }); } catch (e) {}
  };

  /* =====================================================================
     RelayPeer — tương đương Peer của PeerJS.
       new Peer()                 → peer khách (id ngẫu nhiên)
       new Peer('duogames-<CODE>')→ peer host (giữ chỗ phòng trên RTDB)
     ===================================================================== */
  function RelayPeer(id, opts) {
    Emitter.call(this);
    this.id = null;
    this.open = false;
    this.destroyed = false;
    this.disconnected = false;
    this._conns = [];
    this._isHost = false;
    this._code = null;
    this._sess = randStr(16);
    this._heartbeat = null;
    this._hostListenRef = null;
    this._hostListenCb = null;
    this._dead = false;

    if (typeof id === 'string' && id) {
      this._wantId = id;
      this._isHost = true;
      this._code = roomCodeOf(id);
      this._claimHost();
    } else {
      this.id = 'relay-' + randStr(20);
      var self = this;
      setTimeout(function () {
        if (self._dead) return;
        self.open = true;
        self._emit('open', self.id);
      }, 0);
    }
  }
  RelayPeer.prototype = Object.create(Emitter.prototype);

  RelayPeer.prototype._forgetConn = function (conn) {
    var i = this._conns.indexOf(conn);
    if (i >= 0) this._conns.splice(i, 1);
  };

  /* ---- Host: giữ chỗ phòng ---- */
  RelayPeer.prototype._claimHost = function () {
    var self = this;
    var hostRef = roomsRef(this._code).child('host');
    hostRef.once('value').then(function (snap) {
      if (self._dead) return;
      var v = snap.val();
      var now = Date.now();
      if (v && typeof v.ts === 'number' && (now - v.ts) < STALE_MS && v.sess !== self._sess) {
        /* Phòng đang có host sống giữ → giống PeerJS 'unavailable-id' */
        self._emit('error', { type: 'unavailable-id' });
        return;
      }
      self.id = self._wantId;
      try { hostRef.onDisconnect().remove(); } catch (e) {}
      hostRef.set({ ts: serverTs(), sess: self._sess }).then(function () {
        if (self._dead) { // bị destroy trong lúc đang claim: nhả chỗ
          hostRef.remove();
          return;
        }
        self.open = true;
        self._heartbeat = setInterval(function () {
          try { hostRef.update({ ts: serverTs() }); } catch (e) {}
        }, HEARTBEAT_MS);
        self._listenConns();
        self._emit('open', self.id);
      }, function () {
        if (!self._dead) self._emit('error', { type: 'network' });
      });
    }, function () {
      if (!self._dead) self._emit('error', { type: 'network' });
    });
  };

  /* ---- Host: phát hiện kết nối mới ---- */
  RelayPeer.prototype._listenConns = function () {
    var self = this;
    var connsRef = roomsRef(this._code).child('conns');
    this._hostListenRef = connsRef;
    this._hostListenCb = connsRef.on('child_added', function (snap) {
      if (self._dead) return;
      var v = snap.val();
      if (!v || v.b !== self.id) return; // không phải nối tới host này
      var connId = snap.key;
      for (var i = 0; i < self._conns.length; i++) {
        if (self._conns[i].connectionId === connId) return; // đã xử lý
      }
      var conn = new RelayConn(self, self._code, connId, v.a, 'b');
      self._conns.push(conn);
      conn._listen();
      conn._announcePresence();
      self._emit('connection', conn);
      /* 'open' bắn sau khi game đã gắn handler trong sự kiện 'connection' */
      setTimeout(function () { conn._markOpen(); }, 0);
    });
  };

  /* ---- Khách: tạo kết nối tới host ---- */
  RelayPeer.prototype.connect = function (remoteId, opts) {
    var code = roomCodeOf(remoteId);
    var connId = 'c' + randStr(18);
    var conn = new RelayConn(this, code, connId, remoteId, 'a');
    this._conns.push(conn);
    var self = this;
    if (this._dead) {
      setTimeout(function () { conn._emit('error', { type: 'peer-destroyed' }); }, 0);
      return conn;
    }
    /* Kiểm tra host có đang sống không: không có claim tươi → 'peer-unavailable'
       ở cấp Peer (giống PeerJS), conn treo tới khi game tự dọn/thử lại. */
    roomsRef(code).child('host').once('value').then(function (snap) {
      if (conn._closed || self._dead) return;
      var v = snap.val();
      var alive = v && typeof v.ts === 'number' && (Date.now() - v.ts) < STALE_MS;
      if (!alive) {
        self._emit('error', { type: 'peer-unavailable' });
        return;
      }
      var nodeRef = conn._nodeRef();
      try { nodeRef.onDisconnect().remove(); } catch (e) {}
      nodeRef.set({ a: self.id, b: remoteId, ts: serverTs() }).then(function () {
        if (conn._closed) { try { nodeRef.remove(); } catch (e) {} return; }
        conn._listen();
        conn._announcePresence();
        /* 'open' sẽ bắn khi presence của host (p/b) xuất hiện — xem _listen */
      }, function () {
        if (!conn._closed) conn._emit('error', { type: 'network' });
      });
    }, function () {
      if (!conn._closed) conn._emit('error', { type: 'network' });
    });
    return conn;
  };

  RelayPeer.prototype.destroy = function () {
    if (this._dead) return;
    this._dead = true;
    this.destroyed = true;
    this.open = false;
    if (this._heartbeat) { clearInterval(this._heartbeat); this._heartbeat = null; }
    if (this._hostListenRef && this._hostListenCb) {
      try { this._hostListenRef.off('child_added', this._hostListenCb); } catch (e) {}
      this._hostListenRef = null; this._hostListenCb = null;
    }
    var conns = this._conns.slice();
    for (var i = 0; i < conns.length; i++) {
      try { conns[i].close(); } catch (e) {}
    }
    this._conns = [];
    if (this._isHost && this._code) {
      /* Nhả chỗ phòng — chỉ khi claim vẫn là của phiên này */
      var self = this;
      var hostRef = roomsRef(this._code).child('host');
      hostRef.once('value').then(function (snap) {
        var v = snap.val();
        if (v && v.sess === self._sess) { try { hostRef.remove(); } catch (e) {} }
      }, function () {});
    }
  };

  /* Tương thích tối thiểu: games không dùng, rtc-diag (PeerJS cũ) không dùng file này */
  RelayPeer.prototype.disconnect = function () { this.disconnected = true; };
  RelayPeer.prototype.reconnect = function () { this.disconnected = false; };

  if (typeof window !== 'undefined') window.Peer = RelayPeer;
})();
