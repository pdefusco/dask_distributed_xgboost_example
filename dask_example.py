import cdsw, time
import pandas as pd
from faker import Faker
from dask.dataframe import from_pandas


def await_workers(ids, wait_for_completion=True, timeout_seconds=60):

    """await_workers

 

    Description

    -----------

 

    Waits for workers to either reach the 'running' status, or to

    complete and exit.

 

    Parameters

    ----------

    ids: list of worker id's

        The id's of the worker engines to await. The id's are

        available in the "id" key of worker descriptions returned

        by launch_workers and list_workers.

    wait_for_completion: boolean, optional

        If True, will wait for all workers to exit successfully.

        If False, will wait for all workers to reach the 'running'

        status.

        Defaults to True.

    timeout_seconds: int, optional

        Maximum number of seconds to wait for workers to reach the

        desired status. Defaults to 60. If equal to 0, there is no

        timeout. Workers that have not reached the desired status

        by the timeout will be returned in the 'failures' key. See

        the return value documentation.

 

    Returns

    -------

    dict

        A dict with keys 'workers' and 'failures'. The 'workers'

        key contains a list of dicts describing the workers that

        reached the desired status. The 'failures' key contains a

        list of descriptions of the workers that did not.

 

        Note: If wait_for_completion is False, the workers in the

        'workers' key will contain a key called 'ip_address'

        which contains each worker's external IP address. This can be

        useful for running distributed frameworks on workers.

    """

    t = 0

    poll_interval = 5

    out = {"workers": [], "failures": []}

    running = set()

    ids_to_await = set(ids)

    while timeout_seconds != 0 and t <= timeout_seconds:

        workers_now = cdsw.list_workers()

        status_dict = dict([(worker["id"], worker["status"]) for worker in workers_now])

        done = True

        for worker in workers_now:

            id_ = worker["id"]

            if id_ not in ids_to_await:

                continue

            if status_dict[id_] == "failed":

                out["failures"].append(worker)

                ids_to_await.remove(id_)

            elif status_dict[id_] == "timedout":

                out["failures"].append(worker)

                ids_to_await.remove(id_)

            elif status_dict[id_] == "stopped":

                out["failures"].append(worker)

                ids_to_await.remove(id_)

            else:

                if wait_for_completion:

                    if status_dict[id_] == "succeeded":

                        # If the worker has reached the 'succeeded' status,

                        # and we are waiting for completion, it is a success.

                        out["workers"].append(worker)

                        ids_to_await.remove(id_)

                    if status_dict[id_] != "succeeded":

                        # If the worker is in a non-terminal state, and we

                        # are waiting for completion, exit the loop iteration

                        # and wait.

                        done = False

                        break

                else:

                    if status_dict[id_] == "succeeded":

                        # We want the workers to reach 'running', so this is a

                        # failure.

                        out["failures"].append(worker)

                        ids_to_await.remove(id_)

                    elif (

                        status_dict[id_] == "running"

                        and worker.get("ip_address") != "unknown"

                    ):

                        # If the worker has reached the 'running' status, and we

                        # are not waiting for completion, it is a success.

                        out["workers"].append(worker)

                        ids_to_await.remove(id_)

                    else:

                        # If the worker is in a non-terminal state but is not

                        # running, we need to exit the loop iteration and wait.

                        done = False

                        break

        if done:

            return out

        else:

            time.sleep(poll_interval)

            t = t + poll_interval

 

    # Here we have timed out. All workers that are not successes

    # are considered failures.

    if len(ids_to_await) > 0:

        workers_now = cdsw.list_workers()

        for worker in workers_now:

            if worker["id"] in ids_to_await:

                out["failures"].append(worker)

 

    return out


import os, subprocess, cdsw, socket, time

 

# Since CDSW engines have independent IP addresses and

# their own port ranges, it's simpler to hardcode the

# scheduler port.

default_scheduler_port = 2323


def scheduler_address(scheduler_port=default_scheduler_port):

    return "%s:%d" % (os.environ["CDSW_IP_ADDRESS"], scheduler_port)
  

  
def run_scheduler(scheduler_port=default_scheduler_port):

    """

    Run a Dask scheduler process in the background.

 

    The dashboard will be available in the CDSW nine-dot menu

    at the upper right corner of the app.

    """

    scheduler_proc = subprocess.Popen(

        [

            "dask-scheduler",

            "--host",

            os.environ["CDSW_IP_ADDRESS"],

            "--port",

            str(scheduler_port),

            "--dashboard-address",

            ("127.0.0.1:%s" % os.environ["CDSW_READONLY_PORT"]),

        ],

        stdout=subprocess.PIPE,

        stderr=subprocess.STDOUT,

    )

 

    # Wait for the scheduler to become ready

    print("Waiting for Dask scheduler to become ready...")

    while True:

        try:

            with socket.create_connection(

                (os.environ["CDSW_IP_ADDRESS"], scheduler_port), timeout=1.0

            ):

                break

        except OSError as ex:

            time.sleep(0.01)

    print("Dask scheduler is ready")

    return scheduler_proc

def run_dask_workers(

    n, cpu, memory, nvidia_gpu=0, scheduler_port=default_scheduler_port

):

    """

    Run a CDSW worker, and run a Dask worker inside it.

    Assumes that the scheduler is running on the CDSW master.

    """

    worker_code = f"""

import subprocess

import os

print("Starting Dask worker")

scheduler_port={scheduler_port}

worker_proc=subprocess.Popen([

  "dask-worker",

  ("%s:%d" % (os.environ["CDSW_MASTER_IP"], scheduler_port))                       

], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print(worker_proc.wait())

"""

    workers = cdsw.launch_workers(

        n=n,

        cpu=cpu,

        memory=memory,

        nvidia_gpu=nvidia_gpu,  # kernel="python3", \

        code=worker_code,

    )

 

    try:

        ids = [worker["id"] for worker in workers]

 

    except KeyError as key:

        errors = [[worker["k8sMessage"], worker["engineId"]] for worker in workers]

        for error in errors:

            print(

                """worker {} failed to launch with err message :

              {}""".format(

                    error[1], error[0]

                )

            )

        raise RuntimeError("failed to launch workers with err : " + error[0])

 

    print("IDs", ids)

    # Wait for the workers to start running, but don't wait for them to exit -

    # we want them to stay up for use as daemons.

    await_workers(ids, wait_for_completion=False)

    return workers
  
  
def run_dask_cluster(

    n, cpu, memory, nvidia_gpu=0, scheduler_port=default_scheduler_port

):

    """

    Runs a Dask scheduler in the local session, and runs Dask

    workers in CDSW workers.

    """

    scheduler_proc = run_scheduler(scheduler_port)

    workers = run_dask_workers(

        n=n,

        cpu=cpu,

        memory=memory,

        nvidia_gpu=nvidia_gpu,

        scheduler_port=scheduler_port,

    )

    return {

        "scheduler_proc": scheduler_proc,

        "workers": workers,

        "scheduler_address": scheduler_address(scheduler_port),

    }
  

if __name__ == "__main__":

    cluster = run_dask_cluster(n=2, cpu=2, memory=4, nvidia_gpu=0)

    from dask.distributed import Client

    fake = Faker('en_US')
    df = pd.DataFrame(columns=["RFACT_STATE_VAL", "RFACT_TYPE_VAL"])
    df["RFACT_STATE"] = [fake.state_abbr() for i in range(10000)]
    df["RFACT_TYPE"] = [fake.country_code() for i in range(10000)]
    df["RFACT_STATUS"] = [fake.boolean(chance_of_getting_true=25) for i in range(10000)]

    client = Client(cluster["scheduler_address"])

 

    ddf = from_pandas(df, npartitions=2)

    from dask.distributed import Client, wait, get_worker
    from dask_ml.preprocessing import LabelEncoder

 

    num_workers = len(client.scheduler_info()["workers"])

    le = LabelEncoder()

    encoded_state = le.fit_transform(ddf.RFACT_STATE)

    encoded_status = le.fit_transform(ddf.RFACT_STATUS)

    encoded_type = le.fit_transform(ddf.RFACT_TYPE)

    

    ddf = ddf.assign(

        RFACT_STATE_VAL=encoded_state,

        RFACT_STATUS_VAL=encoded_status,

        RFACT_TYPE_VAL=encoded_type,

    )

    print(ddf.columns)

    X_train = ddf[["RFACT_STATE_VAL", "RFACT_TYPE_VAL"]] #this is Regions data, switch this up to get this working for you

    y_train = ddf["RFACT_STATUS_VAL"]

    import xgboost as xgb

 

    dtrain = xgb.dask.DaskDMatrix(client, X_train, y_train)

    dxgb_param = {

        "tree_method": "hist",

        "objective": "reg:squarederror",

        "learning_rate": 0.05,

        "max_depth": 7,

        "reg_lambda": 3.0,

    }

    bst = xgb.dask.train(

        client,

        dxgb_param,

        dtrain,

        num_boost_round=200,

        verbose_eval=False,

    )

    print(bst)

    cdsw.stop_workers()

 