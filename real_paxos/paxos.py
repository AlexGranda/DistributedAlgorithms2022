#!/usr/bin/env python3
import pickle
import sys
import math
import socket
import struct
import time
import threading
import os
import numpy as np
from message import Message

ACCEPTORS = 3  # if you launch a different number of acceptors, change here
LOSS_PERCENTAGE = 0.25  # PUT ZERO IF YOU WANT TO USE YOUR SCRIPT FOR THE LOSS PERCENTAGE
TIMEOUT_TIMER = 0.15  # timeout for both for proposers and learners


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
    print('-> acceptor', id, ' pid:', os.getpid())
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
                # print(decoded_message)
                if message_instance not in acc_state_dict.keys():
                    # instance doesn't exists for the acceptor yet, we create a new state
                    acc_state_dict[message_instance] = {
                        'rnd': decoded_message.c_rnd,  # highest 1a c_rnd received
                        'v_rnd': 0,  # highest 2b c_rnd received
                        'v_val': None,  # corresponding value
                    }
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_1B_message = Message(message_instance, '1B', rnd=decoded_message.c_rnd, v_rnd=0,
                                                   v_val=None)
                        s_acc.sendto(phase_1B_message.encode(), config['proposers'])
                elif message_instance in acc_state_dict.keys() and decoded_message.c_rnd > \
                        acc_state_dict[message_instance]['rnd']:
                    acc_state_dict[message_instance]['rnd'] = decoded_message.c_rnd
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_1B_message = Message(message_instance, '1B', rnd=decoded_message.c_rnd,
                                                   v_rnd=acc_state_dict[message_instance]['v_rnd'],
                                                   v_val=acc_state_dict[message_instance]['v_val'])
                        s_acc.sendto(phase_1B_message.encode(), config['proposers'])
                # print('acceptor', id, state_dict)
            elif message_phase == '2A' and message_instance in acc_state_dict.keys():  # avoid errors
                # print(decoded_message)
                if decoded_message.c_rnd >= acc_state_dict[message_instance]['rnd']:
                    acc_state_dict[message_instance]['v_rnd'] = decoded_message.c_rnd
                    acc_state_dict[message_instance]['v_val'] = decoded_message.c_val
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_2B_message = Message(message_instance, '2B',
                                                   v_rnd=acc_state_dict[message_instance]['v_rnd'],
                                                   v_val=acc_state_dict[message_instance]['v_val'])
                        s_acc.sendto(phase_2B_message.encode(), config['proposers'])
            # to handle catchup from proposers, send a message to proposers with a modified rnd, equal to proposer's crnd, in order to overwrite values in the proposer
            # proposers will never reach consensus with this value since the crnd of the proposer without this value will be lower than the rnd of the acceptors,
            # then the timeout for the quorum will be triggered, the crnd updated until also this proposer will reach the consensus
            elif message_phase == 'CATCHUP':
                if message_instance in acc_state_dict.keys():
                    # print('ACCEPTORS CATCHUP ON INSTANCE: ', message_instance)
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        phase_1B_message = Message(message_instance, '1B', rnd=decoded_message.c_rnd,
                                                   # should be the crnd of the message, but due to the previous commentis the rnd of the acceptor
                                                   v_rnd=acc_state_dict[message_instance]['v_rnd'],
                                                   v_val=acc_state_dict[message_instance]['v_val'])
                        s_acc.sendto(phase_1B_message.encode(), config['proposers'])


# thread to check instances going out of time for the quorum
def timeout_checker():
    while True:
        #for p_instance in range(len(state_dict)):
        for p_instance in list(state_dict.keys()):
            if time.time() - state_dict[p_instance]['timestamp'] > TIMEOUT_TIMER and not state_dict[p_instance][
                                                                                             'phase'] == 'DECISION':
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
                # print('created new instance: ', len(state_dict), 'for proposer: ', id, 'value: ', msg.decode())
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
                    phase_1A_message = Message(len(state_dict) - 1, '1A',
                                               c_rnd=state_dict[len(state_dict) - 1]['c_rnd'])
                    s.sendto(phase_1A_message.encode(), config['acceptors'])
                # print('proposer', id, state_dict) # phase 1a working

            elif not client_message and len(state_dict) > 0 and Message(0, 'DECODING').decode(msg).instance in list(
                    state_dict.keys()):  # to avoid errors from catchup
                decoded_message = Message(0, 'DECODING').decode(msg)
                message_instance = decoded_message.instance
                message_phase = decoded_message.phase
                message_rnd = decoded_message.rnd
                message_v_rnd = decoded_message.v_rnd
                message_v_val = decoded_message.v_val
                # check the phase of the proposer regarding the instance of the message
                if state_dict[message_instance]['phase'] == '1A':
                    # print(decoded_message)
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
                            phase_2A_message = Message(message_instance, '2A',
                                                       c_rnd=state_dict[message_instance]['c_rnd'],
                                                       c_val=value_to_be_proposed)
                            s.sendto(phase_2A_message.encode(), config['acceptors'])

                elif state_dict[message_instance]['phase'] == '2A':
                    # print(decoded_message)
                    # if message is 1B we don't care anymore, since we already achieved the quorum for phase 1A for this instance
                    if message_phase == '2B' and message_v_rnd == state_dict[message_instance]['c_rnd']:
                        # we coount in the quorum only messages with v_rnd == c_rnd
                        state_dict[message_instance]['quorum2B'] += 1
                        # check quorum reached
                        if state_dict[message_instance]['quorum2B'] == quorum:
                            state_dict[message_instance]['quorum2B'] = -1
                            state_dict[message_instance]['timestamp'] = time.time()
                            value_to_be_proposed = message_v_val
                            state_dict[message_instance]['phase'] = 'DECISION'
                            if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                                phase_DECISION_message = Message(message_instance, 'DECISION',
                                                                 v_val=value_to_be_proposed)
                                s.sendto(phase_DECISION_message.encode(),
                                         config['learners'])
                                # print('Consensus reached by proposer: ', id, ' instance:', message_instance, 'value: ',
                                #      value_to_be_proposed)

                            # print('proposer', id, state_dict)
                # if the message is not 1a nor 2a, it is a catchup message for sure so the else will hande if it is not in the keys starting a 1a phase
                elif message_phase == 'CATCHUP' and state_dict[message_instance]['phase'] == 'DECISION':
                    # print(decoded_message)
                    # from instance val to the last, if not equal to prev call phase 1a
                    # if len(state_dict.keys()) > 0 and message_instance < len(state_dict.keys()): implied by the parent if
                    for c_index in range(message_instance, len(state_dict.keys())):
                        if c_index in state_dict.keys():
                            if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                                phase_DECISION_message = Message(c_index, 'DECISION',
                                                                 v_val=state_dict[c_index]['value'])
                                s.sendto(phase_DECISION_message.encode(),
                                         config['learners'])
                                # print('Catchup proposer: ', id, ' instance:', c_index,
                                #      'value: ', state_dict[c_index]['value'])
                        # else that instance has been missed by proposer, start a 1a phase to get updated by acceptors
                        else:
                            state_dict[c_index] = {
                                'quorum1B': 0,  # quorum counter for phase 2A
                                'quorum2B': 0,  # quorum counter for DECISION
                                'value': None,  # no value to be proposed, it will be overwritten by acceptors
                                'phase': '1A',  # current proposer phase for this instance
                                'k': 0,
                                'k_v_val': None,
                                'timestamp': time.time(),
                                'c_rnd': id
                            }
                            if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                                phase_1A_message = Message(c_index, 'CATCHUP', c_rnd=state_dict[c_index]['c_rnd'])
                                s.sendto(phase_1A_message.encode(), config['acceptors'])
            # else that instance has been missed by proposer, start a 1a phase to get updated by acceptors
            elif len(state_dict) > 0 and Message(0, 'DECODING').decode(msg).instance not in list(state_dict.keys()):
                state_dict[Message(0, 'DECODING').decode(msg).instance] = {
                    'quorum1B': 0,  # quorum counter for phase 2A
                    'quorum2B': 0,  # quorum counter for DECISION
                    'value': None,  # no value to be proposed, it will be overwritten by acceptors
                    'phase': '1A',  # current proposer phase for this instance
                    'k': 0,
                    'k_v_val': None,
                    'timestamp': time.time(),
                    'c_rnd': id
                }
                if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                    phase_1A_message = Message(Message(0, 'DECODING').decode(msg).instance, 'CATCHUP',
                                               c_rnd=state_dict[Message(0, 'DECODING').decode(msg).instance]['c_rnd'])
                    s.sendto(phase_1A_message.encode(), config['acceptors'])


def learner_catchup_timeout():
    last_catchup = time.time()  # to wait for clients the first time
    last_true = -1
    # print('THREAD CALLED')
    while True:
        if time.time() - last_catchup > TIMEOUT_TIMER:
            # print('TIMEOUT TRIGGERED', list(learned_values.keys()), len(list(learned_values.keys())))
            if len(list(learned_values.keys())) > 0:
                if list(learned_values.keys())[0] == 0 and last_true == -1:
                    # print('instance 0:', learned_values[0])
                    print(learned_values[0])
                    last_true = 0
                    sys.stdout.flush()

                elif list(learned_values.keys())[0] != 0 and last_true == -1:
                    catchup_message = Message(0, 'CATCHUP')
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        s_learners.sendto(catchup_message.encode(), config['proposers'])

                elif len(list(learned_values.keys())) > 1 and last_true + 1 < len(
                        list(learned_values.keys())) and last_true > -1:
                    for g in range(last_true + 1, len(list(learned_values.keys()))):
                        if list(learned_values.keys())[g] - 1 == list(learned_values.keys())[g - 1]:
                            current_instance = list(learned_values.keys())[g]
                            # print('instance ' + str(current_instance) + ':', learned_values[current_instance])
                            print(learned_values[current_instance])
                            sys.stdout.flush()
                            last_true = current_instance

                        else:
                            catchup_message = Message(list(learned_values.keys())[g - 1], 'CATCHUP')
                            if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                                s_learners.sendto(catchup_message.encode(), config['proposers'])
                            break

                elif last_true + 1 == len(list(learned_values.keys())):
                    catchup_message = Message(last_true, 'CATCHUP')
                    if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                        s_learners.sendto(catchup_message.encode(), config['proposers'])
            # to update new created learners
            else:
                catchup_message = Message(0, 'CATCHUP')
                if np.random.uniform(low=0.0, high=1.0, size=None) > LOSS_PERCENTAGE:
                    s_learners.sendto(catchup_message.encode(), config['proposers'])

            last_catchup = time.time()


def learner(config, id):
    global s_learners
    global learned_values
    r = mcast_receiver(config['learners'])
    s_learners = mcast_sender()
    learned_values = dict()
    threading.Thread(target=learner_catchup_timeout).start()
    while True:
        msg = None
        msg = r.recv(2 ** 30)
        if msg is not None:
            msg_decoded = Message(0, 'DECODING').decode(msg)  # create instance of Message for the received decision
            if msg_decoded.instance not in list(learned_values.keys()):
                learned_values[msg_decoded.instance] = msg_decoded.v_val
                learned_values = dict(sorted(learned_values.items()))
                # print(learned_values)
            sys.stdout.flush()


def client(config, id):
    print('-> client ', id)
    client_timer = True
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
