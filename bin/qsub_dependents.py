#!/usr/bin/env python
# Copyright (c) 2012 Oliver Beckstein <oliver.beckstein@asu.edu>
# Adapted for use with SLURM by David Dotson <david.dotson@asu.edu>
# Published under the BSD 3-clause licence.
 
"""usage: %prog [options] -- [qsub-options] FILE
 
Submit a string of dependent jobs through the PBS/TORQUE, Gridengine,
(GE), or SLURM queuing system. Either set --number or provide BOTH the
benchmarked performance (--performance), the projected run time and
the wall time limit and the script will compute the number of jobs it
needs to launch.
 
The walltime limit should agree (or at least not exceed the limit set
in the script).

NOTE: Dependent jobs are not guaranteed to run if the previous job does not
exit "successfully." This means that the job should fully complete before
the walltime runs out and it has to be killed by the queuing system.

The syntax for PBS, GE, or SLURM is automatically chosen.
 
Examples:
 
   %prog -N 5  run.ge
   %prog -w 12 -p 15.3 -r 100 -- -l walltime=12:00:00 run.pbs
 
Adding three more jobs after a running one with jobid 12345.nid000016:
 
   %prog -N 3 -a 12345.nid000016 run.pbs
 
"""
 
import distutils.spawn
import subprocess
import re
 
DEFAULT_QUEUING_SYSTEM = "PBS"
 
def detect_queuing_system():
    """Heuristic test for GE, PBS, or SLURM"""
    if distutils.spawn.find_executable("sbatch"):
        return "SLURM"
    elif distutils.spawn.find_executable("qsub"):
        p = subprocess.Popen(['qsub', '--help'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 2 and '-W additional_attributes' in err:
            return 'PBS'
        p = subprocess.Popen(['qsub', '-help'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0 and (out.startswith('GE') or '-hold_jid' in out):
            return "GE"

    return None
 
def qsub_dependents(args, jobid=None, queuing_system=DEFAULT_QUEUING_SYSTEM):
    """Submit jobs with *args*, possibly dependent on *jobid*.
 
    *args* is a list and contains everything on the ``qsub`` command
    line (although job dependency options should not be included).
    """
    if jobid is not None:
        new_args = dependent_job_args(jobid, queuing_system) + args
    else:
        new_args = args
    return qsub(new_args, queuing_system=queuing_system)
 
def qsub(args, queuing_system=DEFAULT_QUEUING_SYSTEM):
    """Submit job with ``qsub args`` and return the jobid.
 
    (*args* is a list.)
    """
    if queuing_system == "SLURM":
        base_cmd = "sbatch"
    else:
        base_cmd = "qsub"

    cmd = [base_cmd] + args
    print ">> " + " ".join(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, errmsg = p.communicate()
    if p.returncode != 0:
        raise OSError(p.returncode, "command %r failed: %s" % (" ".join(cmd), errmsg))
    return get_jobid(output, queuing_system)
 
def get_jobid(s, queuing_system):
    """Process textual output from qsub to get the job id.
 
    GE:
 
      .....
      Your job 2844562 ("ENVtest") has been submitted
               ^^^^^^^
 
    PBS:
 
      332161.gordon-fe2.local
      ^^^^^^^^^^^^^^^^^^^^^^^

    SLURM:

      .....
      Submitted batch job 835291
                          ^^^^^^
 
    """
    if queuing_system == "PBS":
        return s.strip()
    elif queuing_system == "GE":
        m =  re.search('Your job (?P<jobid>\d+) \("(?P<jobname>[^ "]+)"\)', s)
        if m:
            return m.group('jobid').strip()
    elif queuing_system == "SLURM":
        m =  re.search('Submitted batch job (?P<jobid>\d+)', s)
        if m:
            return m.group('jobid').strip()
    else:
        raise ValueError("Unknown queuing system %r" % queuing_system)
    return None
 
def dependent_job_args(jobid, queuing_system):
    templates = {'PBS':  ["-W", "depend=afterok:%s" % jobid],
                 'GE': ["-hold_jid", str(jobid)],
                 'SLURM': ["--dependency=afterok:%s" % jobid],
                 }
    return templates[queuing_system]
 
 
if __name__ == "__main__":
    import optparse
    import math
 
    p = optparse.OptionParser(usage=__doc__)
    p.add_option('-N', '--number', dest="number", type="int", metavar="N",
                 default=3,
                 help="run exactly N jobs in total [%default]")
    p.add_option('-p' ,'--performance', dest='performance', type="float", metavar="PERF",
                 default=None,
                 help="job was benchmarked to run at PERF ns/d")
    p.add_option("-r", "--runtime", dest="runtime", type="float", metavar="TIME",
                 default=100.,
                 help="total run time for the simulation in ns [%default]")
    p.add_option("-w", "--walltime", dest="walltime", type="float", metavar="TIME",
                 default=24,
                 help="walltime (in hours) allowed on the queue; must not be longer than "
                 "the walltime set in the queuing script and really should be the same. "
                 "NOTE: must be provided as a decimal number of hours. [%default]")
    p.add_option("-a", "--append", dest="jobid", metavar="JOBID",
                 default=None,
                 help="make the first job dependent on an already running job "
                 "with job id JOBID. (Typically used in conjunction with --number.)")
 
    opts,args = p.parse_args()
    if len(args) == 0:
        raise ValueError('No queuing script was provided.')
 
    if opts.performance and opts.walltime and opts.runtime:
        days = opts.runtime/float(opts.performance)
        num_jobs = math.ceil(days*24/opts.walltime)
        print "-- Will run %d jobs performing at %g ns/d for desired run time %g ns." % (num_jobs, opts.performance, opts.runtime)
        print "-- Expected real time (excluding waiting): %g days" % days
    else:
        num_jobs = opts.number
        print "-- Will run %d jobs" % num_jobs
 
    queuing_system = detect_queuing_system()
    if queuing_system is None:
        queuing_system = DEFAULT_QUEUING_SYSTEM
        print "WW Could not determine queuing system, choosing the default"
    print "-- Using submission syntax for queuing system %r" % queuing_system
 
    # launch the first job (if options.jobid is not None then it will depend on jobid)
    jobid = qsub_dependents(args, jobid=opts.jobid, queuing_system=queuing_system)
 
    # all further jobs
    for ijob in xrange(1, int(num_jobs)):
        jobid = qsub_dependents(args, jobid=jobid, queuing_system=queuing_system)
 
    print "-- launched %d jobs" % num_jobs
