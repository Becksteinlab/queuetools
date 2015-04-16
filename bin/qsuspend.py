#!/usr/bin/env python
# Written by Oliver Beckstein, 2014
# Placed into the Public Domain

import subprocess
import socket

DEFAULTS = {'queuename': ["workstations.q", "short.q"],
            'machine': socket.getfqdn(),
            'time': "21:00",
            }

class GEqueue(object):
    def __init__(self, name):
        self.name = name
    def issuspended(self):
        """Return ``True`` if the queue is in the s(uspended) state."""
        cmd = subprocess.Popen(["qselect", "-qs", "s", "-q", self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = cmd.communicate()
        # return True if qselect found this queue amongst the suspended ones
        return cmd.returncode == 0
    def suspend(self):
        rc = subprocess.call(["qmod", "-s", self.name])
        return rc == 0
    def unsuspend(self):
        rc = subprocess.call(["qmod", "-us", self.name])
        return rc == 0
    def schedule_unsuspend(self, time="21:00"):
        """Run the 'at' command at *time* to unsuspend the queue."""
        cmd = subprocess.Popen(["at",  str(time)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = cmd.communicate("qmod -us {0}".format(self.name))
        return cmd.returncode

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Suspend the queue on HOSTNAME or MACHINE "
                                     "for the rest of the day (until TIME) or until you run "
                                     "qsuspend again. "
                                     "Note that the executing user has to be a Gridengine "
                                     "admin or the script must be run through 'sudo'.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("machine", metavar="MACHINE", nargs="?",
                        default=DEFAULTS['machine'],
                        help="Fully qualified hostname where the queue QUEUENAME should be suspended")
    parser.add_argument("-q", "--queue-name", metavar="QUEUENAME", nargs='*', dest="queuename",
                        default=DEFAULTS['queuename'],
                        help="Name of the Gridengine queue instance.")
    parser.add_argument("-t", "--time", metavar="TIME", dest="time",
                        default=DEFAULTS['time'],
                        help="Suspended queues are automatically unsuspended at this "
                        "time. Set to 'NEVER' to disable (but please do so only after "
                        "CAREFUL CONSIDERATION --- in general, you should aim at releasing "
                        "the queue for general use as soon as possible.") 

    args = parser.parse_args()

    for queue in args.queuename:
        queuename = queue+"@"+args.machine    
        q = GEqueue(queuename)

        if not q.issuspended():
            # suspend
            success = q.suspend()
            if success:
                print("Suspended queue {0}".format(queuename))
                if args.time.upper() != "NEVER":
                    q.schedule_unsuspend(time=args.time)
                    print("Will automatically unsuspend the queue at {0}".format(args.time))
        else:
            success = q.unsuspend()
            if success:
                print("Unsuspended queue {0}".format(queuename))
