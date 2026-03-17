# Spintop-OpenHTF

Spintop-OpenHTF is an opinionated fork of OpenHTF to bring a more standard approach to hardware testbench development. [The complete documentation is hosted on readthedocs.](https://spintop-openhtf.readthedocs.io/en/latest/)

# Getting Started with spintop-openhtf
[![PyPI version](https://badge.fury.io/py/spintop-openhtf.svg)](https://badge.fury.io/py/spintop-openhtf)

## Python Installation

To install spintop-openhtf, you need Python. We officially support **Python 3.6 and 3.7** for now. At this time, it is unlikely we will support 3.8+ because of breaking changes in dependencies (tornado v4). 

If you already have Python installed on your PC, you can skip this step. Officially supported OSes are:

- **Windows 10**
    
    Install Python using the Windows Installer: [https://www.python.org/downloads/windows/](https://www.python.org/downloads/windows/)

- **Raspbian & Ubuntu**

    Install through apt-get

## IDE: VS Code with Extensions

We use and recommend Visual Studio Code for the development of spintop-openhtf testbenches. Using it will allow you to:

- Seamlessly debug with breakpoints the code you will be writting
- Remotely develop on a Raspberry Pi if you wish to
- Use a modern, extendable and free IDE

### Installation

1. [Download VS Code](https://code.visualstudio.com/download)
2. Run and follow the installation executable
3. Install the [Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python)

## Project Folder, Virtual Env & Installation (Windows)

First, create or select a folder where you want your sources to lie in.

Once you have your base python available, best practices are to create a virtualenv for each testbench you create. We will use `python` as the python executable, but if you installed a separate, non-path python 3 for example, you should replace that with your base executable.

Here are the installation steps on Windows:

1. Create Folder

    ```bat
    mkdir myproject
    cd myproject
    ```

2. Create venv

    ```bat
    # Creates new venv in the folder 'venv'
    python -m venv venv
    venv\Scripts\activate 
    ```

3. Install spintop-openhtf

    ```bat
    python -m pip install spintop-openhtf[server]
    ```

You can validate that the installation succeeded using `python -m examples.hello_world`. This will run the basic OpenHTF hello world example without the spintop layer. The example will ask for the entry of a DUT ID from the user and display a passed test once it is done.


## spintop-openhtf Basic Concepts

### Test plan

In the context of a test bench implementation on spintop-openhtf, the test plan is the object to which the test phases are loaded and which executes them. 

See [Running a First Test Bench](https://spintop-openhtf.readthedocs.io/en/latest/docs/first-testbench.html)


### Test phases

The test phases implement the different test cases. They are defined and loaded in the test plan object which executes them one after the other

See [Test Case Declaration](https://spintop-openhtf.readthedocs.io/en/latest/docs/test-flow/ref.html#test-case-declaration)


### Test Sequences

The test sequences are intermediary levels of test phases between the test plan and the test cases. They can help implement complex test hierarchies.

See [Defining Sequences or PhaseGroups](https://spintop-openhtf.readthedocs.io/en/latest/docs/test-flow/ref.html#defining-sequences-or-phasegroups)

### Trigger phase

The trigger phase refers to the first phase of the test bench, in which the dynamic configuration of the test is loaded. Such information can be for example:

- The operator name

- The board or system serial number

- The board or system device type

- The test type to execute on the board or system

See [Trigger Phase](https://spintop-openhtf.readthedocs.io/en/latest/docs/test-flow/ref.html#trigger-phase)

### Test flow management

Test flow management refers to the dynamic selection of which test cases are executed depending on the inputs given at the beginning of the test bench in the trigger phase and by the results of the test themselves. Such inputs that determine test flow are for example the Device Under Test type and a FAIL result of a critical test.

See [Test Flow Management](https://spintop-openhtf.readthedocs.io/en/latest/docs/test-flow/ref.html#test-flow-management)

### Configuration

The configuration refers to all predetermined parameters used to control the flow or the execution of the test. The different configuration types are:

- The parameters configuring the test station itself, that is parameters changing from station to station and test jig to test jig, such as ip adresses, com port, etc.

- The parameters statically configuring the execution of the test plan, such as for example, the maximum number of iterations for a calibration algorithm.

- The parameters dynamically configuring the execution of the test plan, such as those gathered in the trigger phase.

See  [Static Configuration](https://spintop-openhtf.readthedocs.io/en/latest/docs/config/ref.html#static-configuration) and [Test Station Configuration](https://spintop-openhtf.readthedocs.io/en/latest/docs/config/ref.html#test-station-configuration)


### Forms

Forms are use to interact with the test operator. They permit the implementation of complex dialogs which allow to operator to both execute manual operations on the test jig to allow the test to continue or to input test result data for verification,

See [Implementing a Custom Form](https://spintop-openhtf.readthedocs.io/en/latest/docs/config/ref.html#test-station-configuration)

### Plugs

In the spintop-openhtf context, plugs allow the iteraction of the test logic with the surrounding test equipment. They basically wrap the access to the test equipment automation libraries.

See [Forms and Tester Feedback](https://spintop-openhtf.readthedocs.io/en/latest/docs/form/ref.html)

 
### Criteria & measures

The criteria refer to the thresholds against which measures are compared to declare a test case PASS or FAIL. In the spintop-openhtf context, the measures module implements the criteria and the comparison against the selected values.

See [Test Criteria](https://spintop-openhtf.readthedocs.io/en/latest/docs/criteria/ref.html)


### Results

The spintop-openhtf framework outputs a standardized test result record for each test. 

See [Test Results](https://spintop-openhtf.readthedocs.io/en/latest/docs/results/ref.html)



## Tutorials for Spintop-OpenHTF

[First Testbench Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/first-testbench.html)

[Web Interface Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/web-app/ref.html)

[Forms and Tester Feedback Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/form/ref.html)

[Test Bench Definition Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/test-flow/ref.html)

[Test Bench Documentation Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/doc/ref.html)

[Proposed Project Structure](https://spintop-openhtf.readthedocs.io/en/latest/docs/project-structure/ref.html)

[Test Bench Configuration Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/config/ref.html)

[Plugs Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/plugs/ref.html)

[Test Criteria Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/criteria/ref.html)

[Test Results Tutorial](https://spintop-openhtf.readthedocs.io/en/latest/docs/results/ref.html)

