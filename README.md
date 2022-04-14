# Distributed XGBoost with Dask in Cloudera Machine Learning


### Objective

This is a very simple example to get you started with Distributed XGBoost on Dask. 


### Prerequisites

Before you can run the script, you need to install package dependencies.

Start a new CML Session with the following settings:

Editor: Workbench
Kernel: Python 3.7
Edition: Standard
Version: any version is fine
Enable Spark: not required
Resource Profile: a simple 1 vCPU / 2 GiB Memory session will be fine. No GPUs needed.

Once the session is running, open the terminal at the top right and enter the folliwing command:

"pip3 install -r requirements.txt"

Wait for all dependencies to load and the close the terminal window.


### Executing the script

The script leverages the CML Workers API to instantiate n Kubernetes pods for you. 

Each pod runs a worker process, which in this case is used to run a Dask worker.

The dependencies are distributed to each worker and then Dask is used to distribute a Pandas dataframe.

To run the script once you have downloaded all requirements, open the "dask_example.py" script and simply click the play button.


### Other Useful Links

The Dask-XGBoost documentation is located [here](https://xgboost.readthedocs.io/en/latest/tutorials/dask.html).

The CML documentation is located [here](https://docs.cloudera.com/machine-learning/cloud/product/topics/ml-product-overview.html).

If you are new to CML, we recommend trying this [Crash Course Demo](https://github.com/pdefusco/CML_CrashCourse) or trying out an [AMP](https://docs.cloudera.com/machine-learning/cloud/applied-ml-prototypes/topics/ml-amps-overview.html).



