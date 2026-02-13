# encoding:utf-8

"""
wxbot4_client 消息转换：将 RabbitMQ 收到的 WxMessage JSON 转换为 ChatMessage

MQ 消息格式：
{
    "client_id": "client_001",
    "type": "message",
    "data": {
        "msg_type": "text",
        "sender": "张三",
        "chat_name": "张三",
        "chat_type": "friend",
        "content": "你好",
        "hash": "abc123",
        "timestamp": 1707123456.789,
        "extra": {}
    }
}
"""

import time

from bridge.context import ContextType
from channel.chat_message import ChatMessage


# wxbot4_client msg_type -> ContextType 映射
MSG_TYPE_MAP = {
    "text": ContextType.TEXT,
    "image": ContextType.IMAGE,
    "voice": ContextType.VOICE,
    "video": ContextType.VIDEO,
    "file": ContextType.FILE,
    "link": ContextType.SHARING,
    "location": ContextType.TEXT,
    "emotion": ContextType.TEXT,
}


class Wxbot4Message(ChatMessage):
    """将 wxbot4_client 上报的 MQ 消息转换为 ChatMessage"""

    def __init__(self, raw_payload: dict):
        super().__init__(raw_payload)

        data = raw_payload.get("data", {})
        client_id = raw_payload.get("client_id", "")
        extra = data.get("extra", {})

        # 消息基础信息
        self.msg_id = data.get("hash", str(time.time()))
        self.create_time = int(data.get("timestamp", time.time()))

        # 消息类型映射
        msg_type = data.get("msg_type", "text")
        self.ctype = MSG_TYPE_MAP.get(msg_type, ContextType.TEXT)

        # 消息内容
        if msg_type == "voice" and extra.get("voice_text"):
            self.ctype = ContextType.TEXT
            self.content = extra["voice_text"]
        elif msg_type in ("image", "video", "file") and extra.get("filepath"):
            self.content = extra["filepath"]
        else:
            self.content = data.get("content", "")

        # 发送者和聊天窗口
        sender = data.get("sender", "unknown")
        chat_name = data.get("chat_name", "")
        chat_type = data.get("chat_type", "friend")
        is_group = chat_type == "group"

        self.from_user_id = sender
        self.from_user_nickname = sender
        self.to_user_id = client_id
        self.to_user_nickname = client_id
        self.is_group = is_group
        self.my_msg = (sender == "self" or msg_type == "self" or sender == client_id)

        if is_group:
            self.other_user_id = chat_name
            self.other_user_nickname = chat_name
            self.actual_user_id = sender
            self.actual_user_nickname = sender
            self.is_at = False
        else:
            self.other_user_id = sender
            self.other_user_nickname = sender
            self.actual_user_id = sender
            self.actual_user_nickname = sender

        # 保留原始数据
        self._extra = extra
        self._raw_data = data
