## Project 3: Logs Analysis

### Description of The Project

>You've been hired onto a team working on a newspaper site. The user-facing newspaper site frontend itself, and the database behind it, are already built and running. You've been asked to build an internal reporting tool that will use information from the database to discover what kind of articles the site's readers like.

>The database contains newspaper articles, as well as the web server log for the site. The log has a database row for each time a reader loaded a web page. Using that information, your code will answer questions about the site's user activity.

>The program you write in this project will run from the command line. It won't take any input from the user. Instead, it will connect to that database, use SQL queries to analyze the log data, and print out the answers to some questions.

### Required Software

1. [python3](https://www.python.org/downloads/)
2. [VirtualBox](https://www.virtualbox.org/wiki/Downloads)
3. [Vagrant](https://www.vagrantup.com/downloads.html)
4. [VM Configuration](https://d17h27t6h515a5.cloudfront.net/topher/2017/August/59822701_fsnd-virtual-machine/fsnd-virtual-machine.zip)

### Computer Setup

1. Install __python3__, __VirtualBox__ and __Vagrant__.
2. Download and unzip the __VM Configuration__ files(It may be located in your downloads folder).  This file gives you a folder named __FSND-Virtual-Machine__ that contains the VM configuration files.
3. Download and unzip the **data** for the project [here](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip).  The file inside is called **newsdata.sql** and will need to be located in the **vagrant** directory which is located in the previously downloaded folder **FSND-Virtual-Machine**.

### Project Files

The project contains two (2) main files:

1. *logs_analysis.py* - contains the Python program that is used to run the reporting tool and print out the reports based on the data in the database.
2. *newsdata.sql* - contains the news sites data and is used to build the reporting tool by loading it into the local database.

### How To Execute The Project

1. The first thing we need to do is bring the virtual machine online and then log into it.  We will need to be in the **vagrant** directory to do this.  Remember the **vagrant** directory is located in the folder **FSND-Virtual-Machine**
Execute these commands from the terminal:

```
FSND-Virtual-Machine
$ cd vagrant

vagrant
$ vagrant up

vagrant
$ vagrant ssh
```

2. Load **newsdata.sql** into your local database.  We need to cd into the **vagrant** directory and then execute `psql -d news -f newsdata.sql`.  Running this command will connect to your installed database server and execute the SQL commands in the downloaded file, creating tables and populating them with data. Execute these commands:

```
vagrant@vagrant:~$ cd /vagrant

vagrant@vagrant:/vagrant$ psql -d news -f newsdata.sql

```

The database includes three tables:

* The authors table includes information about the authors of articles.
* The articles table includes the articles themselves.
* The log table includes one entry for each time a user has accessed the site.

3. Run the reporting tool python program (**logs_analysis.py**) and print out the reports based on the data in the database.  Execute this command:

```
vagrant@vagrant:/vagrant$ python3 logs_analysis.py
```
The reporting tool should print the answers to the following questions:

* **What are the most popular three articles of all time?**

* **Who are the most popular article authors of all time?**

* **On which days did more than 1% of requests lead to errors?**

### Created By *Adam Crenshaw*
