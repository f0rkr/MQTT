#! /usr/bin/python3

# Brief Mqtt main program
# Author: Ashad Mohamed (aka. F0rkr)
# **********************************
# This the main file for this project, contain all general function
# to implement mqtt publisher and subscriber.

import socket
import logging
import time
import sys
import os
import traceback
import select
import struct
from sys import stdin

# Default variable values
PORT = 1883
HOST = ""
TYPE_CONNECT = 0x10
TYPE_CONNACK = 0x20
TYPE_PUBLISH = 0x30
TYPE_PUBACK = 0x40
TYPE_SUBSCRIBE = 0x80
TYPE_SUBACK = 0x90
TYPE_DISCONNECT = 224
KEEP_ALIVE = '\x00\x3C'.encode("ascii")

def create_mqtt_publish_msg(topic, value, retain=False):
    #  """
    # Creates a mqtt packet of type PUBLISH with DUP Flag=0 and QoS=0.
    # >>> create_mqtt_publish_msg("temperature", "45", False).hex(" ")
    # '30 0f 00 0b 74 65 6d 70 65 72 61 74 75 72 65 34 35'
    # >>> create_mqtt_publish_msg("temperature", "45", True).hex(" ")
    # '31 0f 00 0b 74 65 6d 70 65 72 61 74 75 72 65 34 35'
    # """

    format = ">BBh{}sh{}s"

    retain_code = 0
    if retain:
        retain_code = 1

    # 0011 0000 : Message Type = Publish ; Dup Flag = 0 ; QoS = 0
    msg_mqtt_control_field = (TYPE_PUBLISH + retain_code)

    msg_topic = topic.encode("utf8")
    msg_value = value.encode("utf8")

    msg_topic_length = len(msg_topic)
    msg_value_length = len(msg_value)

    msg_length = msg_topic_length + msg_value_length

    format = format.format(msg_topic_length, msg_value_length)

    return struct.pack(format, msg_mqtt_control_field, msg_length, msg_topic_length, msg_topic, msg_value_length, msg_value)

def unpack_mqtt_publish_msg(packet):
    # Unpacking MQTT packet of type publish
    # packet format

    format = ">BBh{}sh{}s"

    topic_length = int.from_bytes(packet[2:4], byteorder='big')
    value_length = int.from_bytes(packet[4+topic_length:6+topic_length], byteorder='big')

    format = format.format(topic_length, value_length)

    return struct.unpack(format, packet)


def create_mqtt_connect_msg(client_id):
    # Creates MQTT packet of type CONNECT
    # packet format: fixed header (Control field + length) + variable header + payload
    # MQTT CONNECT packet = control + length + protocol name + Protocol Level + Connect Flags + keep alive +Payload
    # *---------------*--------*-----------------*---------*
    # | Control Field | Length | Variable Header | Payload |
    # *---------------*--------*-----------------*---------*

    msg_mqtt_control_field = TYPE_CONNECT # 1

    protocol_name = "MQTT".encode("ascii")
    protocol_name_length = len(protocol_name) # 2

    msg_payload = client_id.encode("ascii") # ?
    msg_payload_length = len(msg_payload) # 2

    keepAlive = 0x3C # 2
    msg_mqtt_length = 2 + msg_payload_length + protocol_name_length

    format = '>BBh{}shh{}s'
    format = format.format(protocol_name_length, msg_payload_length)

    return struct.pack(format, msg_mqtt_control_field, msg_mqtt_length, protocol_name_length, protocol_name, keepAlive, msg_payload_length, msg_payload)

def unpack_mqtt_connect_msg(packet):
    # Unpack MQTT packet of type CONNECT
    # packet format fixed header (Control field + length) + variable header + payload

    format = ">BBh{}shh{}s"
    protocol_length = int.from_bytes(packet[2:4], byteorder='big')
    payload_length = int.from_bytes(packet[6+protocol_length:8+protocol_length], byteorder='big')

    format = format.format(protocol_length, payload_length)

    return struct.unpack_from(format, packet)




def create_mqtt_connack_msg():
    # Creates MQTT packet of type CONNACK
    # packet format: fixed header (Control field + length)
    # *---------------*--------*
    # | Control Field | Length |
    # *---------------*--------*

    msg_mqtt_control_field = TYPE_CONNACK
    msg_mqtt_length = 0x00
    msg_mqtt_fixed_header = msg_mqtt_control_field + msg_mqtt_length
    msg_mqtt_connack = msg_mqtt_fixed_header
    return msg_mqtt_connack



def create_mqtt_disconnect_msg():
    # Create MQTT packet of type DISCONNECT
    # packet format: fixed header (Control field + length)
    # *---------------*--------*
    # | Control Field | Length |
    # *---------------*--------*

    formatter = '>BB'
    msg_mqtt_control_field = TYPE_DISCONNECT
    msg_mqtt_length = 0x00
    return struct.pack(formatter, msg_mqtt_control_field, msg_mqtt_length)


def unpack_mqtt_disconnect_msg(packet):
    unpacked_packet = []
    frame1 = packet[:1]
    frame2 = packet[1:]
    unpacked_packet.append(int.from_bytes(frame1, byteorder='big'))
    unpacked_packet.append(int.from_bytes(frame2, byteorder='big'))
    return unpacked_packet


def create_mqtt_suback_msg():
    pass


def create_mqtt_subscribe_msg(topic, sub_id):
    format = ">BBh{}sh{}s"

    msg_mqtt_control_field = TYPE_SUBSCRIBE

    msg_topic = topic.encode("utf8")
    msg_sub_id = sub_id.encode("utf8")

    msg_topic_length = len(msg_topic)
    msg_sub_id_length = len(msg_sub_id)

    msg_length = msg_topic_length + msg_sub_id_length

    format = format.format(msg_topic_length, msg_sub_id_length)

    return struct.pack(format, msg_mqtt_control_field, msg_length, msg_topic_length, msg_topic, msg_sub_id_length, msg_sub_id)


def unpack_mqtt_subscribe_msg(packet):
    # Unpacking MQTT packet of type subscribe
    # packet format

    format = ">BBh{}sh{}s"

    topic_length = int.from_bytes(packet[2:4], byteorder='big')
    sub_id_length = int.from_bytes(packet[4+topic_length:6+topic_length], byteorder='big')

    format = format.format(topic_length, sub_id_length)

    return struct.unpack(format, packet)


def run_publisher(addr, topic, pub_id, retain=False):
    # TO-DO Implement publisher client side

    # Creating publisher socket
    publisherSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    publisherSocket.connect(addr)
    i = 0
    while True:
        try:
            data = input()  # Getting data
            if data == "":
                break

            # Generating MQTT publish packet then send it to the broker server
            publisherPacket = create_mqtt_publish_msg(topic, data, retain)
            print(publisherPacket)
            publisherSocket.sendall(publisherPacket)
            time.sleep(0.1)
        except EOFError as e:
            # publisherSocket.send(create_mqtt_disconnect_msg())
            publisherSocket.close()
            break



def run_subscriber(addr, topic, sub_id):
    # TO-Do Implement subscriber client side

    # Creating subscriber socket
    subscriberSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    subscriberSocket.connect(addr)

    # Send MQTT Subscriber packet to broker
    topic_msg = topic.encode("ascii")
    subscriberPacket = create_mqtt_subscribe_msg(topic, sub_id)
    subscriberSocket.send(subscriberPacket)

    # Subscriber event connection loop

    while True:
        data = subscriberSocket.recv(1024)
        if len(data) == 0:
            subscriberSocket.close()
            break
        print(topic + " : " + data.decode("utf8"))

    # Ending the TCP connection with the broker server
    subscriberSocket.close()
    return

def get_packet_type(packet):
    # TO-DO return type of packet

    frame1 = int.from_bytes(packet[:1], byteorder='big')

    return frame1
def get_packet_topic(packet):
    # TO-DO return list of topic

    mqtt_publish_msg = unpack_mqtt_publish_msg(packet)

    return mqtt_publish_msg[3].decode('utf8')

def get_packet_topic_and_value(packet):
    # TO-DO return list of topic and value

    print(packet)
    mqtt_publish_msg = unpack_mqtt_publish_msg(packet)

    return [mqtt_publish_msg[3].decode('utf8'), mqtt_publish_msg[5].decode('utf8')]

def run_server(addr):
    # TO-DO: Create MQTT Server socket
    ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    ServerSocket.bind(addr)
    ServerSocket.listen(1)

    # Creating a non-blocking event loop using select
    socketFds = [ServerSocket]  # Socket Descriptors list
    clients = {}


    # List for subscribers organized with topic
    subscribers = {}

    while True:
        readersList, writersList, unexpectedCondition = select.select(socketFds, [], [])
        for connection in readersList:
            if connection == ServerSocket:
                newConnection, address = ServerSocket.accept()
                clients[newConnection] = address
                print("[CONNECTION]: GOT NEW CONNECTION FROM {0}".format(address))
                socketFds.append(newConnection)
            else:
                packet = connection.recv(1024)
                if len(packet) == 0:
                    clients.pop(connection)
                    socketFds.remove(connection)
                    connection.close()
                    break
                print("[INFO]: New msg from {0} : {1}".format(clients[connection], packet))
                if get_packet_type(packet) == TYPE_SUBSCRIBE:
                    subscribers[get_packet_topic(packet)] = [connection]
                elif get_packet_type(packet) == TYPE_PUBLISH:
                    data_list = get_packet_topic_and_value(packet)
                    if data_list[0] in subscribers:
                        for subs in subscribers[data_list[0]]:
                            subs.send(data_list[1].encode("utf8"))
                elif get_packet_type(packet) == TYPE_DISCONNECT:
                    clients.pop(connection)
                    socketFds.remove(connection)
                    connection.close()
                    for list in subscribers:
                        if connection in subscribers[list]:
                            list.remove(connection)
    return 0
