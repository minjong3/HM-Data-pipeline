from slack_sdk import WebClient
from datetime import datetime

class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)
        
    def _format_message(self, msg_type, msg):
        log_url = msg.get('task_instance').log_url.replace("http://localhost:8080", "ec2IP", 1)
        return f"""
        date: {datetime.today().strftime('%Y-%m-%d')}
        result: {msg_type}!
            task id: {msg.get('task_instance').task_id},
            dag id: {msg.get('task_instance').dag_id},
            log url: {log_url}
        """

    def success_msg(self, msg):
        text = self._format_message("Success", msg)
        self.client.chat_postMessage(channel=self.channel, text=text)

    def fail_msg(self, msg):
        text = self._format_message("Fail", msg)
        self.client.chat_postMessage(channel=self.channel, text=text) 