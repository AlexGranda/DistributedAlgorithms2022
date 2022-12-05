import message
import socket
import sys
import math
import socket
import struct
from message import Message
import pickle


def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock


msg = message.Message(0, '1A', c_rnd=1)
print(msg)
a = msg.encode()
print(type(pickle.loads(a)))
a = message.Message(0, 'DECODING').decode(a)
print(a)

print('-> client ', id)
s = mcast_sender()
value = '13 '
value = value.strip()
print("client: sending %s to proposers" % value)
print(type(value.encode()))
print(value.encode())
print(pickle.loads(value.encode()))
