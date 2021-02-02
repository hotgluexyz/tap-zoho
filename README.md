# tap-zoho

# Installation
In order to setup the environment follow below steps:

- Create virtual environment
    - sudo python -m venv ~/.virtualenvs/hotglue
- Activate new virtual environment
    - source ~/.virtualenvs/hotglue/bin/activate
- Install tap-zoho  
    - cd $HG_HOME/taps/tap-zoho 
    - sudo pip install .
- Install requirements.txt
    - pip install -r $HG_HOME/client_api/requirements.txt

# Execute Unit Tests:

- Activate new virtual environment
    - source ~/.virtualenvs/hotglue/bin/activate
- Go to Unit Test Directory
    - $HG_HOME/taps/tap-zoho/test/
- Execute unit test
    - python -m unittest TestZohoData.py
    - python -m unittest TestZohoDiscover.py
