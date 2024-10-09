# VajraIITB
This is the github repository of Team VajraIITB for ITU-WTSA-2024 Hackathon

The main submission document (submitted during phase 1) is ITU_FinalSubmission.pdf

The python packages required for running the programs are in requirements.txt.<br />
These can be installed using `pip install -r requirements.txt` Consider making a virtual environment first though.

## Phase 2
The `phase2` directory contains the following:
   - clean/
   - test_clean/
   - create_run.sh
   - create_test_run.sh

`clean` directory contains the programs and directories necessary for running the pipeline for real time inference.<br />
`test_clean` directory contains the same but for running the pipeline on some samples taken from the dataset.<br />
`create_run.sh` copies `clean` to `run` directory which can be run instead of clean so that there are no changes anywhere in `clean`<br />
`create_test_run.sh` copies `test_clean` to `test_run`.
`clean/app.py` (or `run/app.py`) and `test_clean/app.py` (or `test_run/app.py`) should be copied to `~/flexric/build/examples/xApp/python3` (Follow [oaic/flexric](https://openaicellular.github.io/oaic/OAIC-2024-Workshop-oai-flexric-documentation.html) for the details behind the setup and on how to run the xApp.

## Phase 1
Some of the programs written for self-testing are in `phase1/code` directory
   -  app.py (xApp)
   -  distributor.py (Distributor node)
   -  model.ipynb (Basic models tried like linear regression, SVM, decision trees, random forests, etc.)
   -  model_neural_net.ipynb (3-layer Dense Neural Network model with ReLU activation)
