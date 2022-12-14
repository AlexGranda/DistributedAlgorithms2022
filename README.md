# Distributed Algorithms SA 2022-2023 Project

Students:

- Davide Casnici
- David Alarcón
- Alexandra Granda

The present implementation takes as base the paxos skeleton provided by the TAs, and operates over it. Our implementation 
of Paxos can be found under the `real_paxos` folder. It has the same file structure as the `fake_paxos` original folder, 
and most of our implementation can be found in the `paxos.py` file.

To run the test 1 of the assignment, you will have to write:
```bash
./run.sh real_paxos x
```
where x should be replaced by the number of values you want to be proposed.
Using 100 and 1000 values, the algorithm behaves properly (checked several tens of times).
Proposing more thousands of values, the buffer gets exhausted, even after implementing
a client timer as you suggested.

To run the test 2 of the assignment, before running the command: 
```bash
./run_2acceptor.sh real_paxos x
```
you will have to change in the `real_paxos/paxos.py` file the amount of the acceptor to be run.
You can do it setting to 2 the variable 'ACCEPTORS' at line 13.
We tried it with x equals to 100 and 1000 values, it worked properly.

To run the test 3 of the assignment, before running the command: 
```bash
./run_1acceptor.sh real_paxos x
```
you will have to change in the `real_paxos/paxos.py` file the amount of the acceptor to be run.
You can do it setting to 1 the variable 'ACCEPTORS' at line 13.
We tried it with x equals to 100 and 1000 values, it worked properly.

To run the test 4 of the assignment, regardless which kind of run file do you want to use,
you can set the percentage of loss message changing the variable 'LOSS_PERCENTAGE' in the `real_paxos/paxos.py` file
at the line 14. We have run ALL the tests with and without the loss percentage to be
sure to have implemented it correctly. Anyway, proposing 1000 messages with a quite high percentage loss
may need more than 15 seconds to allow all the learners to learn all the values, so you should
increase the time in the run.sh file to be sure about it.

To run the test 5 of the assignment, we run test one as usual:
```bash
./run.sh real_paxos x
```
using the command kill -9 PID (the proposers print their PID at the beginning of the program).
We tried with 100 and 1000 values, killing one and then two proposers for each time.
 - Killing two proposers with 1000 values, leads to an incomplete paxos, since not all the values
proposed by clients are learned by learners (as expected).
 - Killing one proposer with 1000 values, leads a complete paxos, since the proposers are
still enough to reach a quorum (as expected).
 - Killing two proposers with 100 values, sometimes still leads to complete paxos,
since the program is so fast that they reach consensus almost instantaneously.
 - Killing one proposer with 100 values, leads a complete paxos, since the proposers are
still enough to reach a quorum (as expected).

To run the test 6 of the assignment, you have to run: 
```bash
./run_catch_up.sh real_paxos x
```


FOR EVERY 'run' file we have extended the waiting time. It was necessary
especially to allow consensus to be reached with 1000 proposed values.

To check the tests we have run the file `check_all.sh`.

In the file `real_paxos/paxos.py`, you can change the variable 'TIMEOUT_TIMER' at line 15, 
to change the timers for the learners and the proposers as well. The lower the timer, the
higher the amount of sent messages, so the buffer could be exhausted sooner.

IMPORTANT:
We have changed the file paxos to `basic_paxos.py`, now the `paxos.py` file contains the code of the optimized paxos algorithm, 
avoiding the phase3 of the proposers. Everything else is equal to the basic paxos algorithm.
We had to modify the timing in all the run.sh files in order to satisfy the 3 properties of consensus, without
enough time (since we had to put a timer on the clients) our implementation is not able to guarantee all the three properties.
Using the times we specified in the run files and in our code our implementation works perfectly up to 2000 values
for all the six tests in the assignment, proposing more values with these timings won't work.
All the tests behaved in the same manner as for the basic paxos.
If you would like to run test 6 with 2000 values, you will have to set all the times currently equal to 10, to 100 seconds.

setting lines in the code
```python
ACCEPTORS = 3  # if you launch a different number of acceptors, change here
LOSS_PERCENTAGE = 0.15  # PUT ZERO IF YOU WANT TO USE YOUR SCRIPT FOR THE LOSS PERCENTAGE
TIMEOUT_TIMER = 0.2  # timeout for both for proposers and learners
```
