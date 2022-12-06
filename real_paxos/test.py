import message
import socket
import sys
import math
import socket
import struct
from message import Message
import pickle
import codecs


def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock


msg = message.Message(0, '1A', c_rnd=1).encode()

state_dict = dict()
state_dict[0] = {
    'quorum1B': 0,  # quorum counter for phase 2A
    'quorum2B': 0,  # quorum counter for DECISION
    'value': Message(0, 'DECODING').decode(msg),  # value to be proposed,
    'phase': '1A',  # current proposer phase for this instance
    'k': 1,
    'k_v_val': None
}

print(sys.getsizeof(msg), 'bytes')
print(sys.getsizeof('0|0|123456789|1A|1|NONE'.encode()), 'bytes')

print('0|0|123456789|1A|1|NONE'.split('|'))

import zlib

a = "this string needs compressing"
print(sys.getsizeof(codecs.encode(a)), 'bytes')

#print(sys.getsizeof(codecs.encode(message.Message(0, '1A', c_rnd=1))), 'bytes')

