import asyncio
import html
import json
import traceback

from aiohttp import web

from core.colors import Color


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BridgeBot — Live Chat</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:'Cascadia Code','JetBrains Mono','Fira Code','Consolas',monospace;height:100vh;display:flex;flex-direction:column}
#header{background:#161b22;padding:10px 18px;border-bottom:1px solid #30363d;display:flex;align-items:center;gap:10px;flex-shrink:0}
#header h1{font-size:15px;font-weight:600;color:#f0883e}
#status{width:10px;height:10px;border-radius:50%;background:#3fb950;display:inline-block;transition:background .3s}
#status.disconnected{background:#f85149}
#chat{flex:1;overflow-y:auto;padding:8px 14px;font-size:13px;line-height:1.5;background:#0d1117}
#chat .msg{padding:1px 0;word-break:break-word;animation:fadeIn .12s ease}
@keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
#chat .msg .time{color:#484f58;font-size:11px;margin-right:8px;-webkit-user-select:none;user-select:none}
#chat .msg .text{display:inline}
#chat .divider{border-top:1px solid #21262d;margin:6px 0}
#input-bar{display:flex;padding:10px 14px;background:#161b22;border-top:1px solid #30363d;gap:8px;flex-shrink:0}
#input-bar input{flex:1;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:10px 14px;border-radius:6px;font-family:inherit;font-size:13px;outline:none;transition:border-color .15s}
#input-bar input:focus{border-color:#f0883e}
#input-bar input::placeholder{color:#484f58}
#input-bar button{background:#f0883e;color:#fff;border:none;padding:10px 18px;border-radius:6px;font-family:inherit;font-size:13px;cursor:pointer;font-weight:600;transition:background .15s}
#input-bar button:hover{background:#d47632}
#input-bar button:active{transform:scale(.97)}
::-webkit-scrollbar{width:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#484f58}
</style>
</head>
<body>
<div id="header"><span id="status"></span><h1>BridgeBot — Live Chat</h1></div>
<div id="chat"></div>
<div id="input-bar"><input type="text" id="input" placeholder="Type a message... (/gc hello)" autofocus><button id="send">Send</button></div>
<script>
const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws');
const chat=document.getElementById('chat'),input=document.getElementById('input'),send=document.getElementById('send'),status=document.getElementById('status');
function esc(t){const d=document.createElement('div');d.appendChild(document.createTextNode(t));return d.innerHTML}
function colorize(t){
const cs={'0':'#000','1':'#00a','2':'#0a0','3':'#0aa','4':'#a00','5':'#a0a','6':'#fa0','7':'#aaa','8':'#555','9':'#55f','a':'#5f5','b':'#5ff','c':'#f55','d':'#f5f','e':'#ff5','f':'#fff'};
const fm={'l':'font-weight:bold','m':'text-decoration:line-through','n':'text-decoration:underline','o':'font-style:italic'};
let r='',st=[],i=0;
while(i<t.length){
if(t[i]==='\\u00a7'&&i+1<t.length){const c=t[i+1].toLowerCase();if(c==='r')st=[];else if(cs[c])st=st.filter(s=>!s.startsWith('color:')),st.push('color:'+cs[c]);else if(fm[c])st.push(fm[c]);i+=2}
else{r+=st.length?'<span style="'+st.join(';')+'">'+esc(t[i])+'</span>':esc(t[i]);i++}
}return r
}
ws.onopen=()=>{status.className=''};
ws.onclose=()=>{status.className='disconnected'};
ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==='chat'){const div=document.createElement('div');div.className='msg';const t=new Date().toLocaleTimeString();div.innerHTML='<span class="time">'+t+'</span><span class="text">'+colorize(d.message)+'</span>';chat.appendChild(div);chat.scrollTop=chat.scrollHeight}};
function sendMsg(){const t=input.value.trim();if(!t)return;ws.send(JSON.stringify({type:'chat',message:t}));input.value='';input.focus()}
send.onclick=sendMsg;input.onkeydown=e=>{if(e.key==='Enter')sendMsg()};
</script>
</body>
</html>"""


class WebUI:
    def __init__(self, port=7509):
        self.port = port
        self.clients = set()
        self._loop = None
        self._queue = asyncio.Queue()
        self.on_chat = None
        self.runner = None
        self._task = None
        self.app = web.Application()
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/ws", self._handle_ws)

    def set_chat_callback(self, callback):
        self.on_chat = callback

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await site.start()
        self._task = asyncio.create_task(self._process_queue())
        print(f"{Color.GREEN}WebUI{Color.RESET} > WebUI started on http://0.0.0.0:{self.port}")

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self.runner:
            await self.runner.cleanup()

    def broadcast(self, message):
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._queue.put(message), self._loop)

    async def _process_queue(self):
        while True:
            message = await self._queue.get()
            if not self.clients:
                continue
            data = json.dumps({"type": "chat", "message": message})
            dead = set()
            for ws in self.clients:
                try:
                    await ws.send_str(data)
                except Exception:
                    dead.add(ws)
            self.clients -= dead

    async def _handle_index(self, request):
        return web.Response(text=HTML_PAGE, content_type="text/html")

    async def _handle_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.clients.add(ws)
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        if data.get("type") == "chat" and self.on_chat:
                            asyncio.create_task(self.on_chat(data["message"]))
                    except (json.JSONDecodeError, KeyError):
                        pass
                elif msg.type == web.WSMsgType.ERROR:
                    break
        except Exception:
            traceback.print_exc()
        finally:
            self.clients.discard(ws)
        return ws
