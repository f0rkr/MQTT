#! /usr/bin/python3

# Brief Mqtt main program
# Author: Ashad Mohamed (aka. F0rkr)
# **********************************
# This the main file for this project, contain all general function
# to implement mqtt publisher and subscriber.

import socket
import logging
import sys
import os
import traceback
import time
import select
from sys import stdin

# Default variable values
PORT = 1883
HOST = ""

def create_mqtt_publish_msg(topic, value, retain=False):
    #  """
    # Creates a mqtt packet of type PUBLISH with DUP Flag=0 and QoS=0.
    # >>> create_mqtt_publish_msg("temperature", "45", False).hex(" ")
    # '30 0f 00 0b 74 65 6d 70 65 72 61 74 75 72 65 34 35'
    # >>> create_mqtt_publish_msg("temperature", "45", True).hex(" ")
    # '31 0f 00 0b 74 65 6d 70 65 72 61 74 75 72 65 34 35'
    # """
    retain_code = 0
    if retain:
        retain_code = 1
    # 0011 0000 : Message Type = Publish ; Dup Flag = 0 ; QoS = 0
    msg_mqtt_flags = (TYPE_PUBLISH + retain_code).to_bytes(1, byteorder='big')
    msg_topic = topic.encode("ascii")
    msg_value = bytes(value, "ascii")
    msg_topic_length = len(msg_topic).to_bytes(2, byteorder='big')
    msg = msg_topic_length + msg_topic + msg_value
    msg_length = len(msg).to_bytes(1, byteorder='big')
    return msg_mqtt_flags + msg_length + msg


def create_mqtt_connect_msg(client_id):
    # Creates MQTT packet of type CONNECT
    # packet format: fixed header (Control field + length) + variable header + payload
    # *---------------*--------*-----------------*---------*
    # | Control Field | Length | Variable Header | Payload |
    # *---------------*--------*-----------------*---------*

    msg_mqtt_control_field = bytes('\x10', "ascii")
    msq_mqtt_length = len(client_id).to_bytes(2, byteorder='big')
    msq_mqtt_connect = msg_mqtt_control_field + msq_mqtt_length
    return msq_mqtt_connect

def create_mqtt_connack_msg():
    # Creates MQTT packet of type CONNACK
    # packet format: fixed header (Control field + length)
    # *---------------*--------*
    # | Control Field | Length |
    # *---------------*--------*


    msg_mqtt_control_field = ""
    msg_mqtt_length = ""
    msg_mqtt_fixed_header = msg_mqtt_control_field + msg_mqtt_length
    msg_mqtt_connack = msg_mqtt_fixed_header
    return msg_mqtt_connack

def create_mqtt_disconnect_msg():
    pass

def create_mqtt_suback_msg():
    pass

def create_mqtt_subscribe_msg():
    pass

def run_publisher(addr, topic, pub_id, retain=False):
    # TO-DO Implement publisher client side

    # Creating publisher socket
    publisherSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    publisherSocket.connect(addr)

    # Generating MQTT publish packet then send it to the broker server
    publisherPacket = create_mqtt_publish_msg(topic, pub_id, retain)
    publisherPacket.send(publisherSocket)

    while True:

    pass


def run_subscriber(addr, topic, sub_id):
    # TO-Do Implement subscriber client side

    # Creating subscriber socket
    subscriberSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    subscriberSocket.connect(addr)

    # Subscriber event connection loop
    data = subscriberSocket.recv(1024)
    while data:
        print("{0} : {1}".format(topic, data))
        data = subscriberSocket.recv(1024)

    # Ending the TCP connection with the broker server
    subscriberSocket.close()
    return


def run_server(addr):
    # TO-DO: Create MQTT Server socket
    ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    ServerSocket.bind(addr)
    ServerSocket.listen(1)

    # Creating a non blocking event loop using select
    socketFds = [ServerSocket] # Socket Descriptors list
    clients = {}
    while True:
        readersList, writersList, unexpectedCondition = select.select(socketFds, [], [])
        for connection in readersList:
           if connection == ServerSocket:
                newConnection, address = ServerSocket.accept()
                clients[newConnection] = address
                socketFds.append(newConnection)
                print("{0} [CONNECTION]: New connection from {1}".format(str(time.time_ns())[0:16], address))
           else:
            try:
                packet = connection.recv(1024).decode("utf8")
                print("{0} [INFO]: New msg from {1} : {2}".format(str(time.time_ns())[0:16], clients[connection], packet),flush="True", end="")
            except Exception as e:
                print("{0} [INFO]: Client {1} disconnected".format(str(time.time_ns())[0:16], clients[connection]))
                clients.pop(connection)
                socketFds.remove(connection)
                connection.close()
    return 0