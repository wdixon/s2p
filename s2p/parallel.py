#!/usr/bin/env python
# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@polytechnique.org>

import os
import sys
import traceback
import multiprocessing
from signal import signal, alarm, SIGALRM, SIGKILL
import psutil

from s2p import common
from s2p.config import cfg


def show_progress(a):
    """
    Print the number of tiles that have been processed.

    Args:
        a: useless argument, but since this function is used as a callback by
            apply_async, it has to take one argument.
    """
    show_progress.counter += 1
    status = "done {:{fill}{width}} / {} tiles".format(show_progress.counter,
                                                       show_progress.total,
                                                       fill='',
                                                       width=len(str(show_progress.total)))
    #if show_progress.counter < show_progress.total:
    #    status += chr(8) * len(status)
    #else:
    #    status += '\n'
    print(status, flush=True)
    #sys.stdout.flush()

class Alarm(Exception):
    def __init__(self):
        self.pid = os.getpid()
        process = psutil.Process()
        self.children = []
        for c in process.children(recursive=True):
            self.children.append(c._pid)


def tilewise_wrapper2(fun, *args, **kwargs):

    def alarm_handler(signum, frame):
        print("IN alarm_hadler", flush=True)
        raise Alarm

    signal(SIGALRM, alarm_handler)
    alarm(kwargs['timeout'])
    
    """
    """
    if not cfg['debug']:  # redirect stdout and stderr to log file
        f = open(kwargs['stdout'], 'a')
        sys.stdout = f
        sys.stderr = f

    try:
        q = kwargs['q']
        pid = os.getpid()
        q.put(pid)

        out = fun(*args)
    except Exception:
        print("Exception in %s" % fun.__name__)
        traceback.print_exc()
        raise
    finally:
        alarm(0)

    common.garbage_cleanup()
    if not cfg['debug']:  # close logs
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

    return out

def tilewise_wrapper(fun, *args, **kwargs):
    """
    """
    if not cfg['debug']:  # redirect stdout and stderr to log file
        f = open(kwargs['stdout'], 'a')
        sys.stdout = f
        sys.stderr = f

    try:
        out = fun(*args)
    except Exception:
        print("Exception in %s" % fun.__name__)
        traceback.print_exc()
        raise

    common.garbage_cleanup()
    if not cfg['debug']:  # close logs
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

    return out



def launch_calls(fun, list_of_args, nb_workers, timeout=60, *extra_args):
    """
    Run a function several times in parallel with different given inputs.

    Args:
        fun: function to be called several times in parallel.
        list_of_args: list of (first positional) arguments passed to fun, one
            per call
        nb_workers: number of calls run simultaneously
        extra_args (optional): tuple containing extra arguments to be passed to
            fun (same value for all calls)

    Return:
        list of outputs
    """
    results = []
    queues = []
    outputs = []
    show_progress.counter = 0
    show_progress.total = len(list_of_args)
    pool = multiprocessing.Pool(nb_workers)
    m = multiprocessing.Manager()
    for x in list_of_args:
        q = m.Queue()
        if type(x) == tuple:  # we expect x = (tile_dictionary, pair_id)
            args = (fun,) + x + extra_args
            log = os.path.join(x[0]['dir'], 'pair_%d' % x[1], 'stdout.log')
        else:  # we expect x = tile_dictionary
            args = (fun, x) + extra_args
            log = os.path.join(x['dir'], 'stdout.log')
        queues.append(q)
        results.append(pool.apply_async(tilewise_wrapper2, args=args,
            kwds={'stdout': log, 'timeout': timeout, 'q': q},
            callback=show_progress))

    for idx, r in enumerate(results):
        try:
            pid = queues[idx].get()
            outputs.append(r.get(timeout + 5)) # 13*60))  # wait at most 10 min per call
        except common.RunFailure as e:
            print("FAILED call: ", e.args[0]["command"])
            print("\toutput: ", e.args[0]["output"])
        except multiprocessing.TimeoutError:
            print("Timeout while running %s" % str(r), flush=True)
            try:
                os.kill(pid, SIGKILL)
            except Exception as ex:
                pass
        except Alarm as e:
            print("ALARM RAISED")
            try:
                os.kill(e.pid, SIGKILL)
            except Exception as ex:
                pass
            for pid in e.children:
                try:
                    os.kill(pid, SIGKILL)
                except Exception as ex:
                    pass
        except KeyboardInterrupt:
            pool.terminate()
            sys.exit(1)

        except Exception as e:
            pass

        finally:
            outputs.append(None)

    pool.terminate()
    pool.join()
    common.print_elapsed_time()
    return outputs


def launch_calls_simple(fun, list_of_args, nb_workers, *extra_args):
    """
    Run a function several times in parallel with different given inputs.

    Args:
        fun: function to be called several times in parallel.
        list_of_args: list of (first positional) arguments passed to fun, one
            per call
        nb_workers: number of calls run simultaneously
        extra_args (optional): tuple containing extra arguments to be passed to
            fun (same value for all calls)

    Return:
        list of outputs
    """
    results = []
    outputs = []
    show_progress.counter = 0
    show_progress.total = len(list_of_args)
    pool = multiprocessing.Pool(nb_workers)
    for x in list_of_args:
        if type(x) == tuple:
            args = x + extra_args
        else:
            args = (x,) + extra_args
        results.append(pool.apply_async(fun, args=args, callback=show_progress))

    for r in results:
        try:
            outputs.append(r.get(600))  # wait at most 10 min per call
        except multiprocessing.TimeoutError:
            print("Timeout while running %s" % str(r))
            outputs.append(None)
        except common.RunFailure as e:
            print("FAILED call: ", e.args[0]["command"])
            print("\toutput: ", e.args[0]["output"])
            outputs.append(None)
        except KeyboardInterrupt:
            pool.terminate()
            sys.exit(1)

    pool.close()
    pool.join()
    common.print_elapsed_time()
    return outputs
