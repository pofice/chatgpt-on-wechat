# encoding:utf-8

"""
wxbot4 channel - 通过 RabbitMQ 对接 wxbot4_client

架构：
  [wxbot4_client (Windows)] --RabbitMQ--> [本 channel] --LLM--> [回复通过 RabbitMQ]--> [wxbot4_client 发送]

MQ 拓扑（与 wxbot4_client 保持一致）：
  - Exchange: wxbot.command (direct, durable)
  - Queue: wxbot.message (durable) — 消费来自客户端的消息
  - Queue: wxbot.status (durable) — 消费客户端状态
  - Queue: wxbot.command.{client_id} — 发布指令给客户端
"""

import json
import threading
import time
import uuid

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from bridge.context import *
from bridge.reply import *
from channel.chat_channel import ChatChannel
from channel.wxbot4.wxbot4_message import Wxbot4Message
from common.log import logger
from common.singleton import singleton
from config import conf

# MQ 常量，与 wxbot4_client 保持一致
EXCHANGE_COMMAND = "wxbot.command"
QUEUE_MESSAGE = "wxbot.message"
QUEUE_STATUS = "wxbot.status"


@singleton
class Wxbot4Channel(ChatChannel):
    NOT_SUPPORT_REPLYTYPE = [ReplyType.VOICE, ReplyType.VIDEO_URL, ReplyType.MINIAPP]

    def __init__(self):
        super().__init__()
        self.client_id = conf().get("wxbot4_client_id", "client_001")

        # RabbitMQ 连接参数
        self._mq_config = {
            "host": conf().get("wxbot4_rabbitmq_host", "localhost"),
            "port": conf().get("wxbot4_rabbitmq_port", 5672),
            "username": conf().get("wxbot4_rabbitmq_username", "guest"),
            "password": conf().get("wxbot4_rabbitmq_password", "guest"),
            "vhost": conf().get("wxbot4_rabbitmq_vhost", "/"),
        }

        # 发布用连接（线程安全）
        self._pub_connection = None
        self._pub_channel = None
        self._pub_lock = threading.Lock()

        # 消息去重
        self._received_msgs = {}

    def _get_connection_params(self):
        credentials = pika.PlainCredentials(
            self._mq_config["username"],
            self._mq_config["password"],
        )
        return pika.ConnectionParameters(
            host=self._mq_config["host"],
            port=self._mq_config["port"],
            virtual_host=self._mq_config["vhost"],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=300,
        )

    def _setup_topology(self, channel):
        """声明 exchange 和队列，与 wxbot4_client 拓扑一致"""
        channel.exchange_declare(
            exchange=EXCHANGE_COMMAND,
            exchange_type="direct",
            durable=True,
        )
        queue_name = f"wxbot.command.{self.client_id}"
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            queue=queue_name,
            exchange=EXCHANGE_COMMAND,
            routing_key=self.client_id,
        )
        channel.queue_declare(queue=QUEUE_MESSAGE, durable=True)
        channel.queue_declare(queue=QUEUE_STATUS, durable=True)

    def _ensure_publisher(self):
        """确保发布者连接可用"""
        if self._pub_connection is None or self._pub_connection.is_closed:
            params = self._get_connection_params()
            self._pub_connection = pika.BlockingConnection(params)
            self._pub_channel = self._pub_connection.channel()
            self._setup_topology(self._pub_channel)
        if self._pub_channel is None or self._pub_channel.is_closed:
            self._pub_channel = self._pub_connection.channel()
            self._setup_topology(self._pub_channel)

    def _publish_command(self, action: str, data: dict):
        """发布指令到 wxbot4_client"""
        command = {
            "action": action,
            "request_id": str(uuid.uuid4()),
            "data": data,
        }
        body = json.dumps(command, ensure_ascii=False)

        with self._pub_lock:
            for attempt in range(3):
                try:
                    self._ensure_publisher()
                    self._pub_channel.basic_publish(
                        exchange=EXCHANGE_COMMAND,
                        routing_key=self.client_id,
                        body=body.encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json",
                        ),
                    )
                    logger.info(f"[wxbot4] 已发布指令: {action}, data: {json.dumps(data, ensure_ascii=False)[:100]}")
                    return
                except (AMQPConnectionError, AMQPChannelError) as e:
                    logger.warning(f"[wxbot4] 发布指令失败 (attempt {attempt + 1}): {e}")
                    self._pub_connection = None
                    self._pub_channel = None
                    if attempt < 2:
                        time.sleep(1)
            logger.error(f"[wxbot4] 发布指令最终失败: {body[:200]}")

    def startup(self):
        """启动 channel：连接 RabbitMQ 并开始消费消息"""
        logger.info(f"[wxbot4] 正在启动 wxbot4 channel, client_id={self.client_id}")
        logger.info(f"[wxbot4] RabbitMQ: {self._mq_config['host']}:{self._mq_config['port']}")

        # 启动消息消费线程
        t = threading.Thread(target=self._consume_messages, name="Wxbot4MQConsumer", daemon=True)
        t.start()

        # 启动状态消费线程
        t2 = threading.Thread(target=self._consume_status, name="Wxbot4StatusConsumer", daemon=True)
        t2.start()

        logger.info("[wxbot4] wxbot4 channel 已启动，等待消息...")

    def _consume_messages(self):
        """消费 wxbot.message 队列（自动重连）"""
        while True:
            try:
                params = self._get_connection_params()
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                self._setup_topology(channel)
                channel.basic_qos(prefetch_count=1)

                def on_message(ch, method, properties, body):
                    try:
                        raw = body.decode("utf-8")
                        payload = json.loads(raw)
                        msg_type = payload.get("type", "")

                        if msg_type == "message":
                            self._on_wx_message(payload)
                        elif msg_type == "command_result":
                            req_id = payload.get("request_id", "")
                            success = payload.get("success", False)
                            logger.info(f"[wxbot4] 指令执行结果: request_id={req_id}, success={success}")
                        else:
                            logger.debug(f"[wxbot4] 忽略未知消息类型: {msg_type}")

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except Exception as e:
                        logger.error(f"[wxbot4] 处理消息异常: {e}", exc_info=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=QUEUE_MESSAGE, on_message_callback=on_message, auto_ack=False)
                logger.info(f"[wxbot4] 开始消费 {QUEUE_MESSAGE} 队列")
                channel.start_consuming()

            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.warning(f"[wxbot4] MQ 消息消费连接断开: {e}，5秒后重连...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"[wxbot4] MQ 消息消费异常: {e}", exc_info=True)
                time.sleep(5)

    def _consume_status(self):
        """消费 wxbot.status 队列，记录客户端在线状态"""
        while True:
            try:
                params = self._get_connection_params()
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                self._setup_topology(channel)
                channel.basic_qos(prefetch_count=1)

                def on_status(ch, method, properties, body):
                    try:
                        raw = body.decode("utf-8")
                        payload = json.loads(raw)
                        client_id = payload.get("client_id", "")
                        status = payload.get("status", "")
                        info = payload.get("info", {})
                        nickname = info.get("nickname", "")

                        if status == "online":
                            logger.info(f"[wxbot4] 客户端上线: {client_id} ({nickname})")
                            self.name = nickname
                            self.user_id = client_id
                        elif status == "offline":
                            logger.warning(f"[wxbot4] 客户端离线: {client_id}")
                        elif status == "heartbeat":
                            logger.debug(f"[wxbot4] 心跳: {client_id}")
                        else:
                            logger.debug(f"[wxbot4] 未知状态: {status}")

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except Exception as e:
                        logger.error(f"[wxbot4] 处理状态消息异常: {e}", exc_info=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=QUEUE_STATUS, on_message_callback=on_status, auto_ack=False)
                logger.info(f"[wxbot4] 开始消费 {QUEUE_STATUS} 队列")
                channel.start_consuming()

            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.warning(f"[wxbot4] MQ 状态消费连接断开: {e}，5秒后重连...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"[wxbot4] MQ 状态消费异常: {e}", exc_info=True)
                time.sleep(5)

    def _on_wx_message(self, payload: dict):
        """处理从 wxbot4_client 收到的微信消息"""
        try:
            data = payload.get("data", {})
            msg_hash = data.get("hash", "")

            # 消息去重
            if msg_hash and msg_hash in self._received_msgs:
                return
            if msg_hash:
                self._received_msgs[msg_hash] = time.time()
                self._clean_expired_msgs()

            # 转换为 ChatMessage
            cmsg = Wxbot4Message(payload)
            logger.info(f"[wxbot4] 收到消息: chat={cmsg.other_user_nickname}, "
                        f"sender={cmsg.from_user_nickname}, type={cmsg.ctype}, is_group={cmsg.is_group}, "
                        f"my_msg={cmsg.my_msg}, content={cmsg.content[:80]}")
            logger.debug(f"[wxbot4] 原始 data={data}")

            # 构造 context 并入队处理
            context = self._compose_context(
                cmsg.ctype,
                cmsg.content,
                isgroup=cmsg.is_group,
                msg=cmsg,
            )
            if context:
                self.produce(context)

        except Exception as e:
            logger.error(f"[wxbot4] 处理微信消息异常: {e}", exc_info=True)

    def _clean_expired_msgs(self, expire_time: float = 60):
        """清理过期的消息去重记录"""
        now = time.time()
        for msg_id in list(self._received_msgs.keys()):
            if now - self._received_msgs[msg_id] > expire_time:
                del self._received_msgs[msg_id]

    def send(self, reply: Reply, context: Context):
        """
        发送回复：将 Reply 转为 wxbot4_client 的 Command 格式，
        通过 RabbitMQ 发布到 wxbot.command.{client_id}
        """
        receiver = context.get("receiver")
        if not receiver:
            logger.error("[wxbot4] receiver 为空，无法发送")
            return

        cmsg = context.get("msg")
        # 使用 chat_name 作为发送目标（wxbot4_client 的 send_text 需要 who 参数）
        chat_name = receiver
        if cmsg:
            chat_name = cmsg.other_user_nickname or receiver

        try:
            if reply.type in (ReplyType.TEXT, ReplyType.TEXT_):
                # 文本回复（群聊 @ 前缀已由 _decorate_reply 添加到 content 中）
                self._publish_command("send_text", {
                    "who": chat_name,
                    "content": reply.content,
                })

            elif reply.type in (ReplyType.IMAGE, ReplyType.IMAGE_URL):
                self._publish_command("send_image", {
                    "who": chat_name,
                    "filepath": reply.content,
                })

            elif reply.type == ReplyType.FILE:
                self._publish_command("send_file", {
                    "who": chat_name,
                    "filepath": reply.content,
                })

            elif reply.type == ReplyType.ERROR or reply.type == ReplyType.INFO:
                self._publish_command("send_text", {
                    "who": chat_name,
                    "content": reply.content,
                })

            else:
                logger.warning(f"[wxbot4] 不支持的回复类型: {reply.type}，降级为文本")
                if reply.content:
                    self._publish_command("send_text", {
                        "who": chat_name,
                        "content": str(reply.content),
                    })

        except Exception as e:
            logger.error(f"[wxbot4] 发送回复异常: {e}", exc_info=True)
