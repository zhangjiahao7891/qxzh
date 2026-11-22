// ============================================================
//  穷小子打工记 · 联机服务器（零依赖，纯 Node.js）
//  启动：node server.js
//  端口：process.env.PORT 或 3000
// ============================================================
'use strict';

const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// 普通 HTTP 请求：用于浏览器访问时提示服务器在运行
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('穷小子打工记 联机服务器运行中\n\n把游戏页面带上 ?server= 参数指向本服务器即可联机。');
});

// 在线玩家表：id -> { id, name, x, y, facing, moving }
const clients = new Map();

server.on('upgrade', (req, socket, head) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );

  const id = Math.random().toString(36).slice(2) + Date.now().toString(36);
  const client = {
    id, socket, alive: true,
    name: null, x: 2000, y: 900, facing: 'down', moving: false,
    buffer: head && head.length ? Buffer.from(head) : Buffer.alloc(0),
  };
  clients.set(id, client);

  socket.on('data', (chunk) => {
    client.alive = true;                    // 收到任何数据（含自动 pong）都算活着
    client.buffer = Buffer.concat([client.buffer, chunk]);
    processFrames(client);
  });
  socket.on('close', () => {
    clients.delete(client.id);              // 用当前 id（join 后可能已被改成 MY_ID）
    if (client.name) broadcast({ type: 'leave', id: client.id, name: client.name });
  });
  socket.on('error', () => {});
});

// 解析 WebSocket 帧（客户端 -> 服务器，带掩码）
function processFrames(client) {
  const buf = client.buffer;
  let offset = 0;
  while (offset + 2 <= buf.length) {
    const b0 = buf[offset], b1 = buf[offset + 1];
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let headerLen = 2;

    if (len === 126) {
      if (offset + 4 > buf.length) break;
      len = buf.readUInt16BE(offset + 2);
      headerLen = 4;
    } else if (len === 127) {
      if (offset + 10 > buf.length) break;
      len = Number(buf.readBigUInt64BE(offset + 2));
      headerLen = 10;
    }

    let maskKey = null;
    if (masked) {
      if (offset + headerLen + 4 > buf.length) break;
      maskKey = buf.slice(offset + headerLen, offset + headerLen + 4);
      headerLen += 4;
    }
    if (offset + headerLen + len > buf.length) break;

    let payload = buf.slice(offset + headerLen, offset + headerLen + len);
    if (masked && maskKey) {
      const out = Buffer.alloc(len);
      for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i & 3];
      payload = out;
    }
    offset += headerLen + len;

    if (opcode === 0x8) { // 关闭
      client.socket.end();
      return;
    } else if (opcode === 0x9) { // ping
      sendFrame(client.socket, Buffer.alloc(0), 0xA);
    } else if (opcode === 0x1 && fin) { // 文本消息
      handleMessage(client, payload.toString('utf8'));
    }
  }
  client.buffer = buf.slice(offset);
}

// 发送 WebSocket 帧（服务器 -> 客户端，不带掩码）
function sendFrame(socket, payload, opcode) {
  opcode = opcode || 0x1;
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  socket.write(Buffer.concat([header, payload]));
}

function send(client, obj) {
  if (client.socket.writable) sendFrame(client.socket, Buffer.from(JSON.stringify(obj)));
}
function broadcast(obj, exceptId) {
  const data = Buffer.from(JSON.stringify(obj));
  for (const [id, c] of clients) {
    if (id !== exceptId && c.socket.writable) sendFrame(c.socket, data);
  }
}

// 心跳：每 15 秒 ping 一次；连续两轮没响应（关页面没断干净）就清理幽灵连接
setInterval(() => {
  for (const [cid, c] of clients) {
    if (!c.alive) {
      clients.delete(cid);
      if (c.name) broadcast({ type: 'leave', id: cid, name: c.name });
      try { c.socket.destroy(); } catch (e) {}
      continue;
    }
    c.alive = false;
    try { sendFrame(c.socket, Buffer.alloc(0), 0x9); } catch (e) {}
  }
}, 15000);

function handleMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (e) { return; }
  switch (msg.type) {
    case 'join': {
      const name = String(msg.name || '玩家').slice(0, 12);
      // 踢掉同名旧连接（刷新 / 重登 / 关页面没断干净 → 消除分身）
      for (const [oid, oc] of [...clients]) {
        if (oid !== client.id && oc.name === name) {
          clients.delete(oid);
          try { oc.socket.destroy(); } catch (e) {}
          broadcast({ type: 'leave', id: oid, name: oc.name });
        }
      }
      if (msg.id && typeof msg.id === 'string' && msg.id.length <= 64 && !clients.has(msg.id)) {
        clients.delete(client.id);
        client.id = msg.id;
        clients.set(client.id, client);
      }
      client.name = name;
      client.x = Number(msg.x) || 2000;
      client.y = Number(msg.y) || 900;
      client.facing = msg.facing || 'down';
      send(client, {
        type: 'init',
        players: [...clients.values()].map(p => ({
          id: p.id, name: p.name, x: p.x, y: p.y, facing: p.facing, moving: p.moving,
        })),
      });
      broadcast({ type: 'join', id: client.id, name: client.name, x: client.x, y: client.y, facing: client.facing, moving: false }, client.id);
      break;
    }

    case 'move':
      client.x = Number(msg.x) || client.x;
      client.y = Number(msg.y) || client.y;
      client.facing = msg.facing || client.facing;
      client.moving = !!msg.moving;
      broadcast({ type: 'move', id: client.id, x: client.x, y: client.y, facing: client.facing, moving: client.moving }, client.id);
      break;

    case 'chat':
      if (!client.name) break;
      broadcast({ type: 'chat', id: client.id, name: client.name, text: String(msg.text || '').slice(0, 200) }, client.id);
      break;
  }
}

server.listen(PORT, () => {
  console.log('穷小子打工记 联机服务器已启动：ws://0.0.0.0:' + PORT);
  console.log('游戏页面请使用  ?server=ws://你的IP:' + PORT + '  连接');
});
