# Sales Analysis API

This project provides a RESTful API for analyzing historical sales data. It uses **Node.js**, **Express**, and **PostgreSQL** to serve data from a database and perform various revenue and product analysis.

## Prerequisites

Before you can run this project, ensure you have the following installed:

- **Node.js**: Required to run the backend server.
- **npm**: Node.js package manager, used to install dependencies.
- **PostgreSQL**: Database for storing sales data and performing queries.

## Installation Guide

### 1. **Install Node.js**

If you don't have Node.js installed, follow these steps:

Interviewer note:
used internet to develop this and its not fully tested... 
Reason : i dont have db readily in my local to test this(within 2 hrs)... so i hjave no choice to do this way by using online forums 

#### On Windows:
1. Download the Node.js installer from [here](https://nodejs.org/).
2. Run the installer and follow the instructions to install Node.js and npm.
3. Verify the installation:
   ```bash
   node -v
   npm -v
4. install posthre db and create connetion and give the connection details in dbconnect.js file
5. open postgre sql and run the create scripts form schema.sql file and ensure db is created succesfully
6. give npm install from terminal
7. run "nom start dev" from terminal
8. to load data hit > http:localhost:3000/api/refresh (POST) call from any rest api clients like postman or thunder