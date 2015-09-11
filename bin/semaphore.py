#!/usr/bin/env python

usage = """Query and modify the current state of the resources available on a host.

Subcommands:
    *request*
        request and claim quantities of each type of resource
    *gmxify*
        get job resources as an mdrun input string
    *clear*
        clear resources in use by the given job id

"""
import argparse
import fcntl
import yaml
import subprocess
import socket
from functools import wraps
import os
import sys
import re
import py

class File(object):
    """File object base class. Implements file locking and reloading methods.

    """

    def __init__(self, filename, **kwargs):
        """Create File instance for interacting with file on disk.

        :Arguments:
            *filename*
                name of file on disk object corresponds to

        """
        self.filename = os.path.abspath(filename)
        self.handle = None
        self.fd = None
        self.fdlock = None

        # we apply locks to a proxy file to avoid creating an HDF5 file
        # without an exclusive lock on something; important for multiprocessing
        proxy = "." + os.path.basename(self.filename) + ".proxy"
        self.proxy = os.path.join(os.path.dirname(self.filename), proxy)
        try:
            fd = os.open(self.proxy, os.O_CREAT | os.O_EXCL)
            os.close(fd)
            # set permissions if you can
            try:
                py.path.local(self.proxy).chmod(0777)
            except py.error.EPERM:
                pass
        except OSError:
            pass

    def get_location(self):
        """Get File basedir.

        :Returns:
            *location*
                absolute path to File basedir

        """
        return os.path.dirname(self.filename)

    def _shlock(self, fd):
        """Get shared lock on file.

        Using fcntl.lockf, a shared lock on the file is obtained. If an
        exclusive lock is already held on the file by another process,
        then the method waits until it can obtain the lock.

        :Arguments:
            *fd*
                file descriptor

        :Returns:
            *success*
                True if shared lock successfully obtained
        """
        fcntl.lockf(fd, fcntl.LOCK_SH)

        return True

    def _exlock(self, fd):
        """Get exclusive lock on file.

        Using fcntl.lockf, an exclusive lock on the file is obtained. If a
        shared or exclusive lock is already held on the file by another
        process, then the method waits until it can obtain the lock.

        :Arguments:
            *fd*
                file descriptor

        :Returns:
            *success*
                True if exclusive lock successfully obtained
        """
        fcntl.lockf(fd, fcntl.LOCK_EX)

        return True

    def _unlock(self, fd):
        """Remove exclusive or shared lock on file.

        :Arguments:
            *fd*
                file descriptor

        :Returns:
            *success*
                True if lock removed
        """
        fcntl.lockf(fd, fcntl.LOCK_UN)

        return True

    def _open_fd_r(self):
        """Open read-only file descriptor for application of advisory locks.

        Because we need an active file descriptor to apply advisory locks to a
        file, and because we need to do this before opening a file with
        PyTables due to the risk of caching stale state on open, we open
        a separate file descriptor to the same file and apply the locks
        to it.

        """
        # set permissions if you can
        try:
            py.path.local(self.proxy).chmod(0777)
        except py.error.EPERM:
            pass
            
        self.fd = os.open(self.proxy, os.O_RDONLY)

    def _open_fd_rw(self):
        """Open read-write file descriptor for application of advisory locks.

        """
        # set permissions if you can
        try:
            py.path.local(self.proxy).chmod(0777)
        except py.error.EPERM:
            pass

        self.fd = os.open(self.proxy, os.O_RDWR)

    def _close_fd(self):
        """Close file descriptor used for application of advisory locks.

        """
        # close file descriptor for locks
        os.close(self.fd)
        self.fd = None

    def _open_file_r(self):
        # set permissions if you can
        return open(self.filename, 'r')

    def _open_file_w(self):
        return open(self.filename, 'w')

    def _read(func):
        """Decorator for opening state file for reading and applying shared
        lock.

        Applying this decorator to a method will ensure that the file is opened
        for reading and that a shared lock is obtained before that method is
        executed. It also ensures that the lock is removed and the file closed
        after the method returns.

        """
        @wraps(func)
        def inner(self, *args, **kwargs):
            if self.fdlock:
                out = func(self, *args, **kwargs)
            else:
                self._open_fd_r()
                self._shlock(self.fd)
                self.fdlock = 'shared'

                try:
                    out = func(self, *args, **kwargs)
                finally:
                    self._unlock(self.fd)
                    self._close_fd()
                    self.fdlock = None
            return out

        return inner

    def _write(func):
        """Decorator for opening state file for writing and applying exclusive lock.

        Applying this decorator to a method will ensure that the file is opened
        for appending and that an exclusive lock is obtained before that method
        is executed. It also ensures that the lock is removed and the file
        closed after the method returns.

        """
        @wraps(func)
        def inner(self, *args, **kwargs):
            if self.fdlock == 'exclusive':
                out = func(self, *args, **kwargs)
            else:
                self._open_fd_rw()
                self._exlock(self.fd)
                self.fdlock = 'exclusive'

                try:
                    out = func(self, *args, **kwargs)
                finally:
                    # set permissions if you can
                    try:
                        py.path.local(self.filename).chmod(0777)
                    except py.error.EPERM:
                        pass

                    self._unlock(self.fd)
                    self.fdlock = None
                    self._close_fd()
            return out

        return inner

    def _pull_push(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            try:
                self._pull_record()
            except IOError:
                self._init_record()
            out = func(self, *args, **kwargs)
            self._push_record()
            return out
        return inner

    def _pull(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            self._pull_record()
            out = func(self, *args, **kwargs)
            return out
        return inner

    def _pull_record(self):
        self.handle = self._open_file_r()
        self._record = yaml.load(self.handle)
        self.handle.close()

    def _push_record(self):
        self.handle = self._open_file_w()
        yaml.dump(self._record, self.handle)
        self.handle.close()

    def _init_record(self):
        self._record = dict()
        self._record['resource'] = dict()
        self._record['jobs'] = dict()

    @_write
    @_pull_push
    def populate(self, host, ncore, totcore, ngpu):
        """Build status file elements.

        :Keywords:
            *host*
                hostname of node
            *ncore*
                total number of cores available to queue
            *totcore*
                total number of cores on machine (including hyperthreads)
            *ngpu*
                total number of gpus available to queue
        """
        self._record['resource']['host'] = host
        self._record['resource']['ncore'] = ncore
        self._record['resource']['totcore'] = totcore
        self._record['resource']['ngpu'] = ngpu

    @_write
    @_pull_push
    def request(self, jobid, ncores, ngpus, pinstride=2):
        """Request a number of resources for given job.

        :Arguments:
            *jobid*
                unique id of job claiming resources
            *ncores*
                number of ncores desired
            *ngpus*
                number of gpus desired
            *pinstride*
                minimum pinstride to match
        """
        if jobid in self._record['jobs']:
            raise KeyError("job '{}' already has resources".format(jobid))

        # get resources available
        avail = self._avail()

        ncores_avail = (len(avail['cores']) -
                       (self._record['resource']['totcore'] -
                        self._record['resource']['ncore']))
        ngpus_avail = len(avail['gpus'])

        if (ncores_avail < ncores):
            raise ValueError("not enough cores available")

        # get core configuration
        totcores = self._record['resource']['totcore']

        # iterate through different pinstrides
        # can only get pinstrides up to total cores/desired
        cores_claimed = None
        for i in range(pinstride, totcores/ncores):

            # iterate through possible offsets
            for j in range(0, totcores - (i * ncores) + i, i):
                candidate = range(j, totcores, i)[:ncores]
                # grab the first candidate set of cores that satisfies
                # available set
                if set(avail['cores']).issuperset(set(candidate)):
                    cores_claimed = candidate
                    break

            if cores_claimed:
                break

        if not cores_claimed:
            raise ValueError("no core config matching request could be found")

        # get gpu configuration
        if (ngpus_avail < ngpus):
            raise ValueError("not enough gpus available")


        # take first n gpus available
        gpus_claimed = avail['gpus'][:ngpus]

        self._claim(jobid, cores_claimed, gpus_claimed)

    def _claim(self, jobid, cores, gpus):
        self._record['jobs'][jobid] = dict()
        self._record['jobs'][jobid]['cores'] = cores
        self._record['jobs'][jobid]['gpus'] = gpus

    @_write
    @_pull_push
    def claim(self, jobid, cores, gpus):
        """Claim resources for given job.

        :Arguments:
            *jobid*
                unique id of job claiming resources
            *cores*
                list of core ids to claim
            *gpus*
                list of gpu ids to claim
        """
        self._claim(jobid, cores, gpus)

    def _used(self): 
        used = dict()
        used['cores'] = list()
        used['gpus'] = list()

        for jobid in self._record['jobs']:
            used['cores'].extend(self._record['jobs'][jobid]['cores'])
            used['gpus'].extend(self._record['jobs'][jobid]['gpus'])

        return used

    @_read
    @_pull
    def used(self):
        """Get resources in use.

        :Returns:
            *resources*
                dict giving cores in use as a list of core ids and gpus in
                use as a list of gpu ids; both lists are 0-based

        """
        return self._used()

    def _avail(self):
        used = self._used()
        avail = dict()
        avail['cores'] = list(set(range(self._record['resource']['totcore'])) -
                         set(used['cores']))
        avail['gpus'] = list(set(range(self._record['resource']['ngpu'])) -
                         set(used['gpus']))
            
        return avail

    @_read
    @_pull
    def avail(self):
        """Get available resources.

        :Returns:
            *resources*
                dict giving cores not in use as a list of core numbers and gpus
                not in use as a list of gpu ids; both lists are 0-based

        """
        return self._avail()
    
    @_write
    @_pull_push
    def clear(self, *jobid):
        """Unclaim resources in use by given job.

        :Arguments:
            *jobid*
                unique id(s) of job(s) to unclaim resources for
        """
        for item in jobid:
            self._record['jobs'].pop(item, None)

    @_read
    @_pull
    def get(self, jobid):
        """Get resources in use by given job.

        :Arguments:
            *jobid*
                unique id of job to get in-use resources for

        :Returns:
            *resources*
                dict giving cores in use as a list of core ids and gpus in
                use as a list of gpu ids; both lists are 0-based
        """
        return self._record['jobs'][jobid]

    @_read
    @_pull
    def list(self):
        """Get jobids of active jobs.

        :Returns:
            *jobids*
                list of active jobids
        """
        return self._record['jobs'].keys()

    @_read
    @_pull
    def parse_gmx_mdrun(self, jobid):
        """Get inputs for mdrun for job's core and gpu lists
    
        :Arguments:
            *jobid*
                unique id of job to get in-use resources for
    
        :Returns:
            *mdrunstring*
                string specifying pinstride, pinoffset, and gpuids to use
    
        """
        params = dict()
        cores = self._record['jobs'][jobid]['cores']
        gpus = self._record['jobs'][jobid]['gpus']
        cores.sort()
        gpus.sort()
    
        # get core configuration
        totcores = self._record['resource']['totcore']

        # iterate through different pinstrides to match the sequence
        for i in range(1, totcores/len(cores)):
            if cores == range(cores[0], cores[-1] + i, i):
                params['-pinoffset'] = cores[0]
                params['-pinstride'] = i
                
        if len(gpus) > 0:
            params['-gpu_id'] = "".join([str(x) for x in gpus])
    
        return " ".join(["{} {}".format(k, v) for k, v in params.iteritems()])

class Semaphore(object):
    """Subcommand script interface.

    """
    def __init__(self):
        # file handle
        self.file = File('/scratch/.semaphore.yml')

        parser = argparse.ArgumentParser(
            description='Query and update semaphore for this host.',
            usage=usage)
        parser.add_argument('subcommand', help='subcommand to run')
        # parse_args defaults to [1:] for args, but we need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        subcommand = args.subcommand
        if not hasattr(self, subcommand):
            print 'unrecognized subcommand {}'.format(subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        # send output to stdout
        getattr(self, subcommand)()

    def _populate(self):
        """Call before any subcommand that writes to the semaphore."""
        # get ncores on workstation queue
        p = subprocess.Popen(('qconf', '-sq', 'workstations.q'),
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
        
        out = p.stdout.readlines()
        line = [x for x in out if socket.gethostname() in x][0]
        numcores = int(re.search(r'\[.*=(\d+)\]', line).group(1))

        # get total number of cores on machine
        p = subprocess.Popen(('qconf', '-se', socket.gethostname()),
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
        out = p.stdout.readlines()
        line = [x for x in out if 'processors' in x][0]

        totcores = int(re.search(r'processors *(\d+)', line).group(1))

        # get ngpus
        p = subprocess.Popen(('qconf', '-se', socket.gethostname()),
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
        out = p.stdout.readlines()
        line = [x for x in out if 'gpu' in x]

        # for cases in which the machine has no gpus
        if len(line) != 0:
            line = line[0]
            numgpu = int(re.search(r'.*gpu=(\d+)', line).group(1))
        else:
            numgpu = 0

        return self.file.populate(socket.gethostname(), ncore=numcores, totcore=totcores, ngpu=numgpu)

    def _purge_stale(self):
        """Purge jobs that are no longer running.

        """
        p = subprocess.Popen(('qstat', '-s', 'r'),
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
        
        out = p.stdout.readlines()
        jobids = [x.split()[0] for x in out[2:]]

        dead = list(set(self.file.list()) - set(jobids))
        self.file.clear(*dead)

    def request(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description="""Request quantities of resources from host.""")

        parser.add_argument('--ncores', '-c', default=8, type=int, 
                help='number of cores to request')
        parser.add_argument('--ngpus', '-g', default=1, type=int, 
                help='number of gpus to request')
        parser.add_argument('--pinstride', '-p', default=2, type=int, 
                help='minimum pinstride to use')
        parser.add_argument('jobid', type=str, help='unique id of job')

        args = parser.parse_args(sys.argv[2:])

        self._populate()
        self._purge_stale()

        self.file.request(**vars(args))

    def gmxify(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description="""Get inputs to gmx mdrun for job resources reserved.""")

        parser.add_argument('jobid', help='unique id of job')

        args = parser.parse_args(sys.argv[2:])
        out = self.file.parse_gmx_mdrun(args.jobid)
        print out
        return out

    def clear(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description="""Clear the resource reservation for the given job(s).""")

        parser.add_argument('jobid', help='unique id(s) of job(s)', nargs='+')

        args = parser.parse_args(sys.argv[2:])

        self._populate()
        self.file.clear(*args.jobid)

if (__name__ == '__main__'):
    Semaphore()

