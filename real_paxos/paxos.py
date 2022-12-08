#!/usr/bin/env python3
import pickle
import sys
import math
import socket
import struct
import time
import threading

import numpy as np
from message import Message

ACCEPTORS = 3  # if you want more acceptors, change here
LOSS_PERCENTAGE = 0.25  # PUT ZERO IF YOU WANT TO USE YOUR SCRIPT FOR THE LOSS PERCENTAGE
TIMEOUT_TIMER = 0.15


def mcast_receiver(hostport):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock


def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock


def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, 'r') as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg


# ----------------------------------------------------

def acceptor(config, id):
    print('-> acceptor', id)
    r = mcast_receiver(config['acceptors'])
    s_acc = mcast_sender()
    acc_state_dict = dict()

    while True:
        # init_state = {"rnd": 0, "v-rnd": 0, "v-val": 0}  # acceptor state
        # if instance not in paxos_instances.keys():
        #    paxos_instances[instance] = init_state
        msg = None
        msg = r.recv(2 ** 30)
        if msg is not None:
            decoded_message = Message(0, 'DECODING').decode(msg)
            message_instance = decoded_message.instance
            message_phase = decoded_message.phase

            if message_phase == '1A':
                print(decoded_message)
                if message_instance not in acc_state_dict.keys():
                    # instance doesn't exists for the acceptor yet, we create a new state
                    acc_state_dict[message_instance] = {
                        'rnd': decoded_message.c_rnd,  # highest 1a c_rnd received
                        'v_rnd': 0,  # highest 2b c_rnd received
                        'v_val': None,  # corresponding value
                    }
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_1B_message = Message(message_instance, '1B', rnd=decoded_message.c_rnd, v_rnd=0, v_val=None)
                        s_acc.sendto(phase_1B_message.encode(), config['proposers'])

                else:
                    if decoded_message.c_rnd > acc_state_dict[message_instance]['rnd']:
                        acc_state_dict[message_instance]['rnd'] = decoded_message.c_rnd
                        if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                            phase_1B_message = Message(message_instance, '1B', rnd=decoded_message.c_rnd,
                                                       v_rnd=acc_state_dict[message_instance]['v_rnd'],
                                                       v_val=acc_state_dict[message_instance]['v_val'])
                            s_acc.sendto(phase_1B_message.encode(), config['proposers'])

                # print('acceptor', id, state_dict)
            elif message_phase == '2A':
                print(decoded_message)
                if decoded_message.c_rnd >= acc_state_dict[message_instance]['rnd']:
                    acc_state_dict[message_instance]['v_rnd'] = decoded_message.c_rnd
                    acc_state_dict[message_instance]['v_val'] = decoded_message.c_val
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_2B_message = Message(message_instance, '2B',
                                                   v_rnd=acc_state_dict[message_instance]['v_rnd'],
                                                   v_val=acc_state_dict[message_instance]['v_val'])
                        s_acc.sendto(phase_2B_message.encode(), config['proposers'])


#thread to check instances going out of time for the quorum
def timeout_checker():
    while True:
        for p_instance in range(len(state_dict)):
            if time.time() - state_dict[p_instance]['timestamp'] > TIMEOUT_TIMER and not state_dict[p_instance]['phase'] == 'DECISION':
                state_dict[p_instance]['quorum1B'] = 0
                state_dict[p_instance]['quorum2B'] = 0
                state_dict[p_instance]['phase'] = '1A'
                state_dict[p_instance]['k'] = 1
                state_dict[p_instance]['k_v_val'] = None
                state_dict[p_instance]['timestamp'] = time.time()
                state_dict[p_instance]['c_rnd'] += 2
                if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                    phase_1A_message = Message(p_instance, '1A', c_rnd=state_dict[p_instance]['c_rnd'])
                    s.sendto(phase_1A_message.encode(), config['acceptors'])


def proposer(config, id):
    global state_dict
    global s
    print('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()
    state_dict = dict()
    threading.Thread(target=timeout_checker).start()
    quorum = math.ceil((ACCEPTORS + 1) / 2)

    while True:
        msg = None
        msg = r.recv(2 ** 30)
        if msg is not None:
            client_message = True
            try:
                pickle.loads(msg)
                client_message = False
            except:
                None

            if client_message:
                # we know that the id is unique for each proposer with respect to the instance so we use it directly
                # as c_rnd
                state_dict[len(state_dict)] = {
                    'quorum1B': 0,  # quorum counter for phase 2A
                    'quorum2B': 0,  # quorum counter for DECISION
                    'value': msg.decode(),  # value to be proposed,
                    'phase': '1A',  # current proposer phase for this instance
                    'k': 1,
                    'k_v_val': None,
                    'timestamp': time.time(),
                    'c_rnd': id
                }
                if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                    phase_1A_message = Message(len(state_dict) - 1, '1A', c_rnd=state_dict[len(state_dict) - 1]['c_rnd'])
                    s.sendto(phase_1A_message.encode(), config['acceptors'])
                # print('proposer', id, state_dict) # phase 1a working
            else:
                decoded_message = Message(0, 'DECODING').decode(msg)
                message_instance = decoded_message.instance
                message_phase = decoded_message.phase
                message_rnd = decoded_message.rnd
                message_v_rnd = decoded_message.v_rnd
                message_v_val = decoded_message.v_val

                # check the phase of the proposer regarding the instance of the message
                if state_dict[message_instance]['phase'] == '1A':
                    print(decoded_message)
                    # we already know that the message phase is 1B
                    if message_rnd == state_dict[message_instance]['c_rnd']:
                        state_dict[message_instance]['quorum1B'] += 1
                        if state_dict[message_instance]['k'] < message_v_rnd:
                            state_dict[message_instance]['k'] = message_v_rnd
                            state_dict[message_instance]['k_v_val'] = message_v_val
                            # check quorum reached
                    if state_dict[message_instance]['quorum1B'] == quorum:
                        state_dict[message_instance]['quorum1B'] = -1
                        state_dict[message_instance]['timestamp'] = time.time()
                        value_to_be_proposed = state_dict[message_instance]['value'] if \
                            state_dict[message_instance]['k'] == 1 else \
                            state_dict[message_instance]['k_v_val']
                        state_dict[message_instance]['phase'] = '2A'
                        if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                            phase_2A_message = Message(message_instance, '2A', c_rnd=state_dict[message_instance]['c_rnd'],
                                                       c_val=value_to_be_proposed)
                            s.sendto(phase_2A_message.encode(), config['acceptors'])

                elif state_dict[message_instance]['phase'] == '2A':
                    print(decoded_message)
                    # if message is 1B we don't care anymore, since we already achieved the quorum for phase 1A for this instance
                    if message_phase == '2B' and message_v_rnd == state_dict[message_instance]['c_rnd']:  # we coount in the quorum only messages with v_rnd == c_rnd
                        state_dict[message_instance]['quorum2B'] += 1
                        # check quorum reached
                        if state_dict[message_instance]['quorum2B'] == quorum:
                            state_dict[message_instance]['quorum2B'] = -1
                            state_dict[message_instance]['timestamp'] = time.time()
                            value_to_be_proposed = message_v_val
                            state_dict[message_instance]['phase'] = 'DECISION'
                            phase_DECISION_message = Message(message_instance, 'DECISION', v_val=value_to_be_proposed)
                            s.sendto(phase_DECISION_message.encode(),
                                     config['learners'])
                            print('Consensus reached by proposer: ', id, ' instance:', message_instance)

                            # print('proposer', id, state_dict)


#def learner_catchup_timeout():
#    while True:
#        if time.time() - last_catchup > 1:
#            for k in learned_values.keys():
#                s_learners.sendto(msg_decoded.encode(), config['proposers'])
#                last_catchup = time.time()


def learner(config, id):
    r = mcast_receiver(config['learners'])
    global s_learners
    global last_catchup
    global learned_values
    s_learners = mcast_sender()
    last_catchup = time.time() - 10
    learned_values = dict()
    while True:
        msg = None
        msg = r.recv(2 ** 30)
        if msg is not None:
            if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                msg_decoded = Message(0, 'DECODING').decode(msg)  # create instance of Message for the received decision
                if msg_decoded.instance not in learned_values.keys():
                    learned_values[msg_decoded.instance] = msg_decoded.v_val
                    print(learned_values)
                    #print(msg = msg.v_val)
                    sys.stdout.flush()


def client(config, id):
    print('-> client ', id)
    s = mcast_sender()
    for value in sys.stdin:
        value = value.strip()
        print("client: sending %s to proposers" % value)
        s.sendto(value.encode(), config['proposers'])
    print('client done.')


if __name__ == '__main__':
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    if role == 'acceptor':
        rolefunc = acceptor
    elif role == 'proposer':
        rolefunc = proposer
    elif role == 'learner':
        rolefunc = learner
    elif role == 'client':
        rolefunc = client
    rolefunc(config, id)
