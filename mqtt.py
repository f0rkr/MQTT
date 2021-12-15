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

PORT = 1883

def create_mqtt_publish_msg(topic, value, retain=False):
    """
    Creates a MQTT packet of type PUBLISH with DUP Flag=0 and QoS=0.
    """
    pass


def run_publisher(addr, topic, pub_id, retain=False):
    """
    Run client publisher.
    """
    pass


def run_subscriber(addr, topic, sub_id):
    """
    Run client subscriber.
    """
    pass


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