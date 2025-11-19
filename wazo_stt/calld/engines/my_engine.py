# -*- coding: utf-8 -*-
# Copyright 2025 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import functools
import logging
import websocket
from threading import Thread
#from google.cloud import speech
#from google.cloud.speech import enums
#from google.cloud.speech import types

from .engine_base import SttEngineBase

logger = logging.getLogger(__name__)

def iter_chunks(buf, size=1024):
    mv = memoryview(buf)
    for i in range(0, len(buf), size):
        yield mv[i : i + size]


class Channel:
    def __init__(self, ws: websocket.WebSocketApp, t: Thread) -> None:
        self.ws = ws
        self.thread = t
        pass


class VietSttEngine(SttEngineBase):
    """Google Cloud Speech-to-Text engine implementation"""

    def _initialize(self):
        """Initialize the Google STT client"""
        # self._speech_client = speech.SpeechClient.from_service_account_file(
        #     self._config["stt"]["google_creds"])
        # self._streaming_config = types.StreamingRecognitionConfig(
        #     config=types.RecognitionConfig(
        #         encoding=enums.RecognitionConfig.AudioEncoding.LINEAR16,
        #         sample_rate_hertz=16000,
        #         language_code=self._config["stt"]["language"]))

        self.channels: dict[str, Channel] = {}
        pass

    def process_audio_chunk(self, channel, tenant_uuid, buf):
        """Process an audio chunk through Google STT
        
        Args:
            channel: The channel object
            tenant_uuid: The tenant UUID
            chunk: Binary audio data
        """

        try:
            # if not chunk:
            #     return
            for chunk in iter_chunks(buf, 1024):
                self.channels[channel.id].ws.send_bytes(chunk)
            logger.info(f"{len(chunk)} of {type(chunk)} has been sent")
        except Exception as e:
            logger.error(f"got ERROR: {e}")
            pass

        # with requests.get(url, stream=True) as r:
        #     r.raise_for_status()
        #     for chunk in r.iter_content(chunk_size=1024):
        #         if chunk:  # ignore keep-alive chunks
        #             print("Chunk:", chunk.decode())

        # request = types.StreamingRecognizeRequest(audio_content=chunk)
        # responses = list(self._speech_client.streaming_recognize(
        #     self._streaming_config, [request]))

        # for response in responses:
        #     results = list(response.results)
        #     logger.debug("Google STT results: %d", len(results))
        #     for result in results:
        #         if result.is_final:
        #             transcription = result.alternatives[0].transcript
        #             self.publish_transcription(channel, tenant_uuid, transcription)
        pass

    def on_error(self, ws, exc, channel):
        logger.error(f"Got error for channel: {channel.id}, {exc}")

    def on_close(self, ws, close_status_code, close_msg, channel):
        logger.error(
            f"Done for channel: {channel.id}: {close_msg} with status {close_status_code}"
        )

    def start(self, channel, tenant_uuid, **kwargs):
        """Start processing for a channel
        
        Args:
            channel: The channel to process
            **kwargs: Additional parameters (not used for Google STT)
        """
        logger.info(f"Google STT engine ready for channel: {channel.id}")
        if channel.id in self.channels:
            return True

        try:
            url = self._config["stt"]["stt_server"]
            ws = websocket.WebSocketApp(
                url,
                on_close=functools.partial(self.on_close, channel=channel),
                on_error=functools.partial(self.on_error, channel=channel),
            )

            def run_client():
                ws.run_forever()
                logger.info("run_forever() has returned")

            thread = Thread(target=run_client)

            self.channels[channel.id] = Channel(ws, thread)

            logger.info(f"Done connecting to {url}")
        except Exception as e:
            logger.error(f"got ERROR: {e}")
            return False
        # Google engine doesn't need special initialization per channel
        return True

    def stop(self, channel_id, tenant_uuid):
        """Stop processing for a channel
        
        Args:
            channel_id: ID of the channel to stop
        """
        logger.info(f"Stopping Google STT for channel: {channel_id}")

        try:
            channel = self.channels.pop(channel_id)
            channel.ws.close()
            channel.thread.join()
        except Exception as e:
            logger.error(f"got ERROR: {e}")
            return False
        # Google engine doesn't need special cleanup per channel
        return True