import message
import socket

msg = message.Message(0, '1A', c_rnd=1)
print(msg)
a = msg.encode()

a = message.Message(0, 'DECODING').decode(a)
print(a)
