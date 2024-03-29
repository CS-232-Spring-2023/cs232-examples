# Dogs database demo

To make this program work, you will need to create a file in this directory named `.env` (without the back ticks) ex. `touch .env`. Inside this file you can copy an pasted the data below into the `.env` file:

DATABASE=dogdb
DBHOST=localhost
DBUSERNAME=[your mysql username here]
DBPASSWORD=[your mysql password here]

With the above data copied replace the bracketed username and password strings with your MySQL username and password respectively (again, do not keep the brackets).

The `.env` file will hold your "secrets". The purpose of this is to avoid having sensitive information such as login credentials for your database management system, servers, etc. stored within your source code in plain text. Instead, we will load this content from the `.env` file and use our `.gitignore` file to avoid ever committing this content to the source code repository. Additionally, when working with multiple people, it is highly unlikely you will have the same login credientials for MySQL, so you would be constantly changing the code so that it works for each individual team member. This way, we load the "secrets" from a separate file which is not in the repository and saved locally to each developer's computer.

## Python environment

### Create the environment

`python -m venv ./venv`

or

`python3 -m venv ./venv`

### Activate the environment

* macOS: `source ./venv/bin/activate`
* Windows: `venv\Scripts\activate`

### Install packages

`pip install -r requirements.txt`


### Run main

`python dog_app.py`

#### Deactivate the environment

`deactivate`
