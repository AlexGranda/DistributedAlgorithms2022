#!/usr/bin/env python3
import pickle
import sys
import math
import socket
import struct
from message import Message

ACCEPTORS = 3  # if you want more acceptors, change here


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
    state = {}
    r = mcast_receiver(config['acceptors'])
    s = mcast_sender()
    while True:
        # init_state = {"rnd": 0, "v-rnd": 0, "v-val": 0}  # acceptor state
        # if instance not in paxos_instances.keys():
        #    paxos_instances[instance] = init_state
        msg = r.recv(2 ** 16)
        # fake acceptor! just forwards messages to the learner
        if id == 1:
            # print "acceptor: sending %s to learners" % (msg)
            s.sendto(msg, config['learners'])


def proposer(config, id):
    print('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()
    state_dict = dict()
    quorum = math.ceil((ACCEPTORS + 1) / 2)

    while True:
        msg = None
        msg = r.recv(2 ** 16)

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
                    'quorum': 0,  # quorum counter
                    'value': msg.decode(),  # value to be proposed,
                    'phase': '1A',  # current proposer phase for this instance
                    'k': 0,
                    'k_v_val': None
                }
                phase_1A_message = Message(len(state_dict), '1A', c_rnd=id)
                s.sendto(phase_1A_message, config['acceptors'])

            else:
                decoded_message = Message(0, 'DECODING').decode(msg)
                message_instance = decoded_message.instance
                message_phase = decoded_message.phase
                message_rnd = decoded_message.rnd
                message_v_rnd = decoded_message.v_rnd
                message_v_val = decoded_message.v_val

                # check the phase of the proposer regarding the instance of the message
                if state_dict[message_instance]['phase'] == '1A':
                    # we already know that the message phase is 1B
                    if id == message_rnd:
                        state_dict[message_instance]['quorum'] += 1
                        if state_dict[message_instance]['k'] < message_v_rnd:
                            state_dict[message_instance]['k'] = message_v_rnd
                            state_dict[message_instance]['k_v_val'] = message_v_val
                            # check quorum reached
                    if state_dict[message_instance]['quorum'] == quorum:
                        value_to_be_proposed = msg if state_dict[message_instance]['k'] == 0 else \
                            state_dict[message_instance]['k_v_val']
                        state_dict[message_instance]['phase'] = '2A'
                        phase_2A_message = Message(message_instance, '2A', c_rnd=id,
                                                   c_val=value_to_be_proposed)
                        s.sendto(phase_2A_message.encode(), config['acceptors'])

                elif state_dict[message_instance]['phase'] == '2A':
                    # if message is 1B we don't care anymore, since we already achieved the quorum for phase 1A for this instance
                    if message_phase == '2B' and message_v_rnd == id:  # we coount in the quorum only messages with v_rnd == c_rnd
                        state_dict[message_instance]['quorum'] += 1
                        # check quorum reached
                        if state_dict[message_instance]['quorum'] == quorum:
                            value_to_be_proposed = message_v_val
                            state_dict[message_instance]['phase'] = 'DECISION'
                            phase_DECISION_message = Message(message_instance, 'DECISION', v_val=value_to_be_proposed)
                            s.sendto(phase_DECISION_message.encode(),
                                     config['learners'])  # TODO check how to handle this for the prder


def learner(config, id):
    r = mcast_receiver(config['learners'])
    while True:
        msg = r.recv(2 ** 16)
        msg = Message(0, 'DECODING').decode(msg)  # create instance of Message for the received decision
        msg = msg.v_val  # retrieve only the value
        print(msg)
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
