#!/usr/bin/env python
# Written by Oliver Beckstein, 2014
# Placed into the Public Domain
from __future__ import print_function

import sys
import subprocess
import socket

DEFAULTS = {'queuename': ["workstations.q"],
            'machine': socket.getfqdn(),
            'deltatime': 4,
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
        """Run the 'at' command at *time* to unsuspend the queue.
        
        *time* should be a time string understood by at, e.g., 'now +1 h'
        or 'today 9pm'.
        """
        cmd = subprocess.Popen(["at",  str(time)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = cmd.communicate("qmod -us {0}".format(self.name))
        return cmd.returncode

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Suspend the queue on HOSTNAME or MACHINE "
                                     "until TIME h have passed or until you run "
                                     "qsuspend again. "
                                     "Note that the executing user has to be a Gridengine "
                                     "admin or the script must be run through 'sudo'. "
                                     "If you cannot run it, talk to a sysadmin.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("machine", metavar="MACHINE", nargs="?",
                        default=DEFAULTS['machine'],
                        help="Fully qualified hostname where the queue QUEUENAME should be suspended")
    parser.add_argument("-q", "--queue-name", metavar="QUEUENAME", nargs='*', dest="queuename",
                        default=DEFAULTS['queuename'],
                        help="Name of the Gridengine queue instance.")
    parser.add_argument("-t", "--time", metavar="TIME", type=float, dest="time",
                        default=DEFAULTS['deltatime'],
                        help="Suspended queues are automatically unsuspended after that many hours. "
                        "The maximum allowed value is 8 (hours).")

    args = parser.parse_args()

    # check unsuspend time is reasonable
    if args.time > 8:
        print("Maximum suspend time exceeded: set to 8h")
        args.time = 8.
    elif args.time < 0:
        print("ERROR: Suspend time must be >= 0")
        sys.exit(1)

    for queue in args.queuename:
        queuename = queue+"@"+args.machine    
        q = GEqueue(queuename)

        if not q.issuspended():
            # suspend
            success = q.suspend()
            if success:
                print("Suspended queue {0}".format(queuename))
                minutes = int(args.time * 60)
                q.schedule_unsuspend(time="now + {0} min".format(minutes))
                print("Will automatically unsuspend the queue after {0} hours".format(args.time))
        else:
            success = q.unsuspend()
            if success:
                print("Unsuspended queue {0}".format(queuename))
