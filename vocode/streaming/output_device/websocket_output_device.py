from __future__ import annotations

import asyncio
import json

from fastapi import WebSocket

from vocode.streaming.models.audio import AudioEncoding
from vocode.streaming.models.transcript import TranscriptEvent
from vocode.streaming.models.websocket import AudioMessage, TranscriptMessage
from vocode.streaming.output_device.rate_limit_interruptions_output_device import (
    RateLimitInterruptionsOutputDevice,
)


class WebsocketOutputDevice(RateLimitInterruptionsOutputDevice):
    def __init__(self, ws, sampling_rate: int, audio_encoding: AudioEncoding):
        super().__init__(sampling_rate, audio_encoding)
        self.ws = ws
        self.active = False
        self.queue: asyncio.Queue[str] = asyncio.Queue()

    def start(self):
        self.active = True
        return super().start()

    def mark_closed(self):
        self.active = False

    async def play(self, chunk: bytes):
        message = AudioMessage.from_bytes(chunk).json()
        if isinstance(self.ws, WebSocket):
            await self.ws.send_text(AudioMessage.from_bytes(chunk).json())
        else:
            await self.ws.emit('audio', json.loads(message))

    async def send_transcript(self, event: TranscriptEvent):
        if self.active:
            transcript_message = TranscriptMessage.from_event(event).json()
            if isinstance(self.ws, WebSocket):
                await self.ws.send_text(transcript_message)
            else:
                await self.ws.emit('transcript', json.loads(transcript_message))

    async def consume_interrupt(self, is_interrupt: bool):
        if self.active:
            interrupt_message = json.dumps({"data": is_interrupt, "type": "interrupt"})
            if isinstance(self.ws, WebSocket):
                await self.ws.send_text(interrupt_message)
            else:
                await self.ws.emit('interrupt', interrupt_message)