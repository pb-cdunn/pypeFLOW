from pypeflow.simple_pwatcher_bridge import (PypeProcWatcherWorkflow, Dist)
from pypeflow.tasks import gen_task
#from pypeflow.do_task import wait_for
import logging
import os
import sys
LOG = logging.getLogger(__name__)

TASK_A = """
echo hello > {output.hello}

# params.pypeflow_nproc is special, same as NPROC passed to job-distributor.
echo {params.pypeflow_nproc} >> {output.hello}

cat {input.extra} >> {output.hello}
"""

TASK_B = """
cat {input.hello} > {output.goodbye}
echo {params.pypeflow_nproc} >> {output.goodbye}
echo {params.foo} >> {output.goodbye}
cat {input.hello} >> {output.goodbye}
"""

def Task(script, inputs, outputs, parameters=None, dist=None):
    if parameters is None:
        parameters = dict()
    if dist is None:
        dist = Dist()

    # Make paths relative to CWD. (But ok if caller does this.)
    rel_inputs = dict()
    rel_outputs = dict()
    def get_rel(maybe_abs):
        rel = dict()
        for (k, v) in maybe_abs.items():
            try:
                if os.path.isabs(v):
                    v = os.path.relpath(v)
                rel[k] = v
            except Exception:
                LOG.exception('Error for {!r}->{!r}'.format(k, v))
                raise
        return rel
    inputs = get_rel(inputs)
    outputs = get_rel(outputs)

    first_output_dir = os.path.normpath(os.path.dirname(list(outputs.values())[0]))
    # All outputs must be in same directory.

    params = dict(parameters)
    ## Add 'topdir' parameter for convenience, though we should not need this.
    #rel_topdir = os.path.relpath('.', first_output_dir) # redundant for rel-inputs, but fine
    #params['topdir'] = rel_topdir

    pt = gen_task(script, inputs, outputs, params, dist)

    return pt


def run(wf, start_fn):
    # You can create a Dist anytime you want.
    default_dist = Dist(
        #NPROC=4,
        #MB=4000,
        #local=True,
        #use_tmpdir='/tmp',
    )

    hello_fn = '0-run/hello/hello.txt'
    goodbye_fn = '0-run/goodbye/goodbye.txt'

    wf.addTask(Task(
        script=TASK_A,
        inputs={
            'extra': start_fn,
        },
        outputs={
            'hello': hello_fn,
        },
        dist=Dist(NPROC=2)
    ))

    special_job_dict = dict(
        NPROC=4, MB=8000,
    )

    wf.addTask(Task(
        script=TASK_B,
        inputs={
            'hello': hello_fn,
        },
        outputs={
            'goodbye': goodbye_fn,
        },
        parameters=dict(
            foo='bar',
        ),
        dist=Dist(job_dict=special_job_dict),
    ))

    wf.max_jobs = 16 # Change this whenever you want, before any refresh.
    wf.refreshTargets()

    return goodbye_fn


submit = """
qsub -S /bin/bash -sync y -V  \
        -q ${JOB_QUEUE}     \
        -N ${JOB_NAME}        \
        -o "${JOB_STDOUT}" \
        -e "${JOB_STDERR}" \
        -pe smp ${NPROC}    \
        -l h_vmem=${MB}M    \
        "${JOB_SCRIPT}"
"""

# Simple local-only submit-string.
submit = 'bash -C ${CMD} >| ${STDOUT_FILE} 2>| ${STDERR_FILE}'

def main(prog, start_fn):
    job_defaults = dict(
        njobs=32,
        NPROC=3,
        MB=2000,
        submit=submit,
        job_type='string',
        pwatcher_type='blocking',
    )
    wf = PypeProcWatcherWorkflow(
            job_defaults=job_defaults,
    )
    finish_fn = run(wf, start_fn)
    LOG.info('Finished: {}'.format(finish_fn))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main(*sys.argv)
