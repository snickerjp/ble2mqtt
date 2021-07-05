import asyncio as aio
import json
import logging
import uuid
from dataclasses import dataclass
from enum import Enum

from ..protocols.base import BLEQueueMixin
from .base import COVER_DOMAIN, Device

logger = logging.getLogger(__name__)

COVER_ENTITY = 'cover'

BLINDS_CONTROL = uuid.UUID("0000fe51-0000-1000-8000-00805f9b34fb")


class RunState(Enum):
    OPENED = 'opened'
    OPENING = 'opening'
    CLOSED = 'closed'
    CLOSING = 'closing'
    STOPPED = 'stopped'


@dataclass
class AM43State:
    battery: int = None
    position: int = None
    light: int = None
    run_state: RunState = RunState.STOPPED
    target_position: int = None


class AM43Cover(BLEQueueMixin, Device):
    NAME = 'am43'
    MANUFACTURER = 'Generic'
    DATA_CHAR = BLINDS_CONTROL
    ACTIVE_SLEEP_INTERVAL = 1
    SEND_DATA_PERIOD = 5
    # STANDBY_SEND_DATA_PERIOD_MULTIPLIER = 12 * 5  # 5 minutes
    STANDBY_SEND_DATA_PERIOD_MULTIPLIER = 4

    MIN_POSITION = 0
    MAX_POSITION = 100

    # command IDs
    CMD_MOVE = 0x0d
    CMD_STOP = 0x0a
    CMD_BATTERY = 0xa2
    CMD_LIGHT = 0xaa
    CMD_POSITION = 0xa7

    @property
    def entities(self):
        return {
            COVER_DOMAIN: [
                {
                    'name': COVER_ENTITY,
                    'device_class': 'shade',
                },
            ],
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = AM43State()

    def notification_callback(self, sender_handle: int, data: bytearray):
        # from ble2mqtt.utils import format_binary
        # logger.info(f'[{self}] BLE notification: {sender_handle}: {format_binary(data)}')
        self.process_data(data)
        self._ble_queue.put_nowait((sender_handle, data))

    async def send_command(self, characteristic, id, data: list,
                           wait_reply=True, timeout=25):
        logger.info(f'[{self}] - send command {id:x}{data}')
        cmd = bytearray([0x9a, id, len(data)] + data)
        csum = 0
        for x in cmd:
            csum = csum ^ x
        cmd += bytearray([csum])

        ret = False
        if characteristic:
            self.clear_ble_queue()
            await self.client.write_gatt_char(BLINDS_CONTROL, cmd)
            ret = True
            if wait_reply:
                logger.info(f'[{self}] waiting for reply')
                ble_notification = await aio.wait_for(
                    self.ble_get_notification(),
                    timeout=timeout,
                )
                logger.info(f'[{self}] reply: {ble_notification[1]}')
                ret = bytes(ble_notification[1][3:-1])
        return ret

    async def _request_position(self):
        await self.send_command(self.DATA_CHAR, self.CMD_POSITION, [0x01], True)

    async def _request_state(self):
        await self._request_position()
        await self.send_command(self.DATA_CHAR, self.CMD_BATTERY, [0x01], True)
        await self.send_command(self.DATA_CHAR, self.CMD_LIGHT, [0x01], True)

    async def get_device_data(self):
        await super().get_device_data()
        await self.client.start_notify(
            self.DATA_CHAR,
            self.notification_callback,
        )
        await self._request_state()

    def process_data(self, data: bytearray):
        if data[1] == self.CMD_BATTERY:
            self._state.battery = int(data[7])
        elif data[1] == self.CMD_POSITION:
            self._state.battery = int(data[5])
        elif data[1] == self.CMD_LIGHT:
            self._state.battery = int(data[4]) * 12.5
        else:
            logger.error(f'{self} BLE notificationUnknown identifier not')

    async def _notify_state(self, publish_topic):
        logger.info(f'[{self}] send state={self._state}')
        coros = []

        state = {'linkquality': self.linkquality}
        covers = self.entities.get(COVER_DOMAIN, [])
        for cover in covers:
            if cover['name'] == COVER_ENTITY:
                cover_state = {
                    **state,
                    'state': self._state.run_state.value,
                    'position': self._state.position,
                    'battery': self._state.battery,
                    'light': self._state.light,
                }
                coros.append(publish_topic(
                    topic='/'.join((self.unique_id, cover['name'])),
                    value=json.dumps(cover_state),
                ))
        if coros:
            await aio.gather(*coros)

    async def handle(self, publish_topic, send_config, *args, **kwargs):
        # request every SEND_DATA_PERIOD if running and
        # SEND_DATA_PERIOD * STANDBY_SEND_DATA_PERIOD_MULTIPLIER if in
        # standby mode

        timer = 0
        while True:
            await self.update_device_data(send_config)
            # if running notify every 5 seconds, 60 sec otherwise
            is_running = self._state.run_state in [
                RunState.OPENING,
                RunState.CLOSING,
            ]
            multiplier = (
                1 if is_running else self.STANDBY_SEND_DATA_PERIOD_MULTIPLIER
            )

            timer += self.ACTIVE_SLEEP_INTERVAL
            if timer >= self.SEND_DATA_PERIOD * multiplier:

                if is_running:
                    logger.info(f'[{self}] check for position')
                    await self._request_position()
                    if self._state.run_state == RunState.CLOSING and \
                            self._state.position == self.MAX_POSITION:
                        self._state.run_state = RunState.CLOSED
                    elif self._state.run_state == RunState.OPENING and \
                            self._state.position == self.MIN_POSITION:
                        self._state.run_state = RunState.OPENED
                else:
                    logger.info(f'[{self}] check for full state')
                    await self._request_state()
                await self._notify_state(publish_topic)
                timer = 0
            await aio.sleep(self.ACTIVE_SLEEP_INTERVAL)

    async def handle_messages(self, publish_topic, *args, **kwargs):
        while True:
            try:
                if not self.client.is_connected:
                    raise ConnectionError()
                message = await aio.wait_for(
                    self.message_queue.get(),
                    timeout=60,
                )
            except aio.TimeoutError:
                await aio.sleep(1)
                continue
            value = message['value']
            entity_name, postfix = self.get_entity_from_topic(message['topic'])
            if entity_name == COVER_ENTITY:
                value = self.transform_value(value)
                target_value = None
                if postfix == self.SET_POSTFIX:
                    logger.info(
                        f'[{self}] set mode {entity_name} value={value}',
                    )
                    if value.lower() == 'open':
                        target_value = self.MIN_POSITION
                    elif value.lower() == 'close':
                        target_value = self.MAX_POSITION
                    # assume that rest is 'stop'
                elif postfix == self.POSITION_POSTFIX:
                    logger.info(
                        f'[{self}] set position {entity_name} value={value}',
                    )
                    try:
                        target_value = int(value)
                    except ValueError:
                        pass
                while True:
                    try:
                        if target_value is None:
                            await self.send_command(
                                self.DATA_CHAR,
                                self.CMD_STOP,
                                [0xcc],
                            )
                            self._state.run_state = RunState.STOPPED

                        elif 0 <= target_value <= 100:
                            await self.send_command(
                                self.DATA_CHAR,
                                self.CMD_MOVE,
                                [int(target_value)],
                            )
                            if self._state.position < target_value:
                                self._state.target_position = target_value
                                self._state.run_state = RunState.CLOSING
                            elif self._state.position > target_value:
                                self._state.target_position = target_value
                                self._state.run_state = RunState.OPENING
                            else:
                                self._state.target_position = None
                                self._state.run_state = RunState.STOPPED
                        else:
                            logger.error(
                                f'[{self}] Incorrect position value: '
                                f'{repr(target_value)}',
                            )

                        await self._notify_state(publish_topic)
                        break
                    except ConnectionError as e:
                        logger.exception(str(e))
                    await aio.sleep(5)
