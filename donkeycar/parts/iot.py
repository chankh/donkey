#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 12:32:53 2017

@author: chankh
"""
import os
import sys
import json
import base64
import numpy as np
from datetime import datetime

from io import BytesIO
from PIL import Image

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient


class IoTPublisher():

    def __init__(self, vehicle_id, client, inputs=None, types=None):
        self.vehicle_id = vehicle_id
        self.client = client
        self.connected = self.client.connect()
        if not self.connected:
            raise IOError("Cannot establish connection with AWS IoT")

        # create log and save meta
        self.meta = {'inputs': inputs, 'types': types}
        # with open(self.meta_path, 'w') as f:
        #     json.dump(self.meta, f)

        self.start_time = datetime.now().timestamp()
        self.current_ix = int(round(self.start_time * 1000))

    def run(self, *args):
        '''
        API function needed to use as a Donkey part.

        Accepts values, pairs them with their inputs keys and saves them
        to disk.
        '''
        assert len(self.inputs) == len(args)

        self.record_time = int(datetime.now().timestamp() - self.start_time)
        record = dict(zip(self.inputs, args))
        self.put_record(record)

    def put_record(self, data):
        """
        Save values like images that can't be saved in the csv log and
        return a record with references to the saved values that can
        be saved in a csv.
        """
        json_data = {}
        self.current_ix += 1

        for key, val in data.items():
            typ = self.get_input_type(key)

            if typ in ['str', 'float', 'int', 'boolean']:
                json_data[key] = val

            elif typ == 'image_array':
                img = Image.fromarray(np.uint8(val))
                name = self.make_file_name(key, ext='.jpg')
                buffered = BytesIO()
                img.save(buffered, 'JPEG')
                json_data[key] = name
                json_data['image'] = base64.b64encode(buffered.getvalue()).decode('ascii')

            else:
                msg = 'Tub does not know what to do with this type {}'.format(typ)
                raise TypeError(msg)

        self.write_json_record(json_data)
        return self.current_ix

    def make_file_name(self, key, ext='.png'):
        name = '_'.join([str(self.current_ix), key, ext])
        name = name.replace('/', '-')
        return name

    def write_json_record(self, json_data):
        json_data['current_ix'] = self.current_ix
        json_data['vehicleID'] = self.vehicle_id
        json_data['time'] = datetime.isoformat(datetime.now())
        try:
            r = self.client.publish("AutonomousVehicles/" + self.vehicle_id, json.dumps(json_data), 0)
            if not r:
                print('Unable to publish record:', json_data)
        except TypeError as e:
            print('troubles with record:', e, json_data)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def shutdown(self):
        if self.connected:
            self.client.disconnect()

    @property
    def inputs(self):
        return list(self.meta['inputs'])

    @property
    def types(self):
        return list(self.meta['types'])

    def get_input_type(self, key):
        input_types = dict(zip(self.inputs, self.types))
        return input_types.get(key)


class AWSHandler():

    def __init__(self, vehicle_id, endpoint, ca, private_key, certificate):
        self.vehicle_id = vehicle_id
        self.endpoint = endpoint
        self.ca = os.path.expanduser(ca)
        self.private_key = os.path.expanduser(private_key)
        self.certificate = os.path.expanduser(certificate)

    def create_iot_client(self):
        client = AWSIoTMQTTClient(self.vehicle_id)
        client.configureEndpoint(self.endpoint, 8883)
        client.configureCredentials(self.ca, self.private_key, self.certificate)
        client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        client.configureDrainingFrequency(2)  # Draining: 2 Hz
        client.configureConnectDisconnectTimeout(10)  # 10 sec
        client.configureAutoReconnectBackoffTime(1, 128, 20)
        client.configureMQTTOperationTimeout(5)  # 5 sec
        return client

    def new_iot_publisher(self, inputs, types):
        client = self.create_iot_client()
        iot = IoTPublisher(vehicle_id=self.vehicle_id, client=client, inputs=inputs, types=types)
        return iot


