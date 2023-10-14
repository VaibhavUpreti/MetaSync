#!/usr/bin/env python3
from flask import Flask render_template, request, redirect, url_for, session, flash 
from redis import Redis
from rq import Worker, Queue, Connection
import os
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import bcrypt
from flask_login import UserMixin, login_user, LoginManager, login_required, logout_user, current_user
from sqlalchemy.dialects.postgresql import JSON
import json
import requests
from faker import Faker


login_manager = LoginManager()

app = Flask(__name__)

# Secret Key
app.secret_key = 'base64_32_digit_key'

# DB config
app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:postgres@0.0.0.0:5432/metasync_development'
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Flask Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'users/registration'

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
    }

@login_manager.user_loader
def load_user(user_id):
	return User.query.get(int(user_id))

redis = Redis(host='localhost', port=6379)

# Using URL for redis
#redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
#conn = redis.from_url(redis_url)

@app.route('/')
def hello():
    redis.incr('hits')
    cnt = redis.get('hits').decode("utf-8")
    return render_template('index.html', hits=cnt)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/pricing')
def pricing():
    return render_template('pricing.html')

@app.route('/users/sign_in', methods=['GET', 'POST'])
def sign_in():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        user = User.query.filter_by(email=email).first()
        
        if user and user.check_password(password):
            session['email'] = user.email
            login_user(user)
            user_id = user.id 

            return redirect(url_for('hello'))
            # return redirect(url_for('dashboard', user_id=user_id))
        else:
            return render_template('users/registration.html',error='Invalid user')

    return render_template('users/registration.html')

# Create Logout Page
@app.route('/users/log_out', methods=['GET', 'POST'])
@login_required
def logout():
	logout_user()
	return redirect(url_for('hello'))

@app.route('/users/sign_up', methods=['GET', 'POST'])
def sign_up():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']

        new_user = User(username=username,email=email,password=password)
        db.session.add(new_user)
        db.session.commit()
        login_user(new_user)
        return render_template('index.html')
    else:
        return render_template('users/new.html')

@app.route('/users/<int:user_id>', methods=['GET', 'POST'])
@login_required
def dashboard(user_id):
    
    connections = Connection.query.filter_by(user_id=current_user.id).all()
    plugins = requests.get("http://localhost:8083/connector-plugins/", headers=headers)
    result = plugins.text
    data = json.loads(result)
    return render_template('dashboard.html', connections=connections, plugins=data)

@app.route('/users/<int:user_id>/connections/new', methods=['POST'])
@login_required
def add_db_connection(user_id):
    if request.method == 'POST':
        host = request.form['host']
        port = request.form['port']
        user = request.form['user']
        adapter = request.form['adapter']
        password = request.form['password']
        db_name = request.form['db_name']
        notes = request.form['notes']
        fake = Faker()
        slot_name = fake.user_name()
        debezium_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "topic.prefix": "postgres",
            "database.user": user,
            "database.dbname": db_name,
            "database.hostname": "host.docker.internal", #host,
            "database.password": password,
            "database.server.name": "postgres",
            "database.history.kafka.bootstrap.servers": "kafka:9092",
            "database.history.kafka.topic": "schema-changes.inventory",
            "database.port": port,
            "plugin.name": "pgoutput",
            "slot.name": slot_name,
            "max.poll.records": "99999999999",
            "log.cleaner.enable": "false",
            "max.partition.fetch.bytes": "100000000000000000000000000000000",
            "fetch.max.bytes": "5242880000000000000000000000000000",
            "session.timeout.ms": "5000000000000000"
        }
        debezium_config_json = json.dumps(debezium_config) 
        url = "http://localhost:8083/connectors"
        random_name = fake.user_name()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
            }
        data = json.dumps({
            "name": random_name,
            "config": debezium_config 
            })
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 201:
            flash("Successfully added DB configuration.")
            cuser_id = current_user.id
            conn_name = random_name
            new_connection = Connection(host=host,port=port, user=user, password=password, adapter=adapter, db_name=db_name, notes=notes, debezium_config=debezium_config_json, conn_name= conn_name)
            new_connection.user_id = cuser_id
            db.session.add(new_connection)
            db.session.commit()
            print("success")
        else:
            print("failure")
            flash("Error adding DB configuration. Please check your settings.")
        print(response.text)
        return redirect(url_for('dashboard', user_id = current_user.id))
    else:
        return render_template('connection/new.html')

@app.route('/users/<int:user_id>/connections/<connection_name>', methods=['GET', 'POST'])
@login_required
def show_connection(user_id, connection_name):
    connection = Connection.query.filter_by(conn_name=connection_name)
    url = "http://localhost:8083/connectors/" + connection_name
    headers = {
             "Accept": "application/json",
    
             "Content-Type": "application/json"
        }
    connectors = requests.get(url, headers=headers)
    topics_url = "http://localhost:8083/connectors/" + connection_name + "/topics"
    topics = requests.post(topics_url, headers=headers)

    result = connectors.text
    data = json.loads(result)
    topics = json.loads(topics.text)
    print(data)

    return render_template('connection/show.html', connectors=result, data=data, topics=topics, connection=connection)

@app.route('/users/<int:user_id>/connections/add_s3_sink', methods=['POST'])
def add_s3_sink(user_id):
    url = "http://localhost:8083/connectors"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
    s3_sink = {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "s3.region": "us-west-2",
      "flush.size": "1000",
      "schema.compatibility": "NONE",
      "topics": "postgres.public.users",
      "tasks.max": "1",
      "timezone": "UTC",
      "s3.part.size": "5242880",
      "locale": "PL",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "s3.bucket.name": "circuitverse-development",
      "rotate.schedule.interval.ms": "10000"
    }
    data = json.dumps({
        "name": "s3-sink",
        "config": s3_sink 
        })
    response = requests.post(url, headers=headers, data=data)
    print(response.text)
    return render_template('index.html') 

# Models

class User(db.Model, UserMixin):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(30), nullable=False)
    email = db.Column(db.String(30), unique=True)
    password = db.Column(db.String(100))
    
    # connections = db.relationship('Connection', backref='user_connections')

    # Use a unique backref name like 'owned_connections'
    owned_connections = db.relationship('Connection', backref='owner')

    def __init__(self, username, email, password):
        self.username = username
        self.email = email
        self.password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def __repr__(self):
       return f"<User {self.name}>"

    def check_password(self,password):
        return bcrypt.checkpw(password.encode('utf-8'),self.password.encode('utf-8'))

class Connection(db.Model, UserMixin):
    __tablename__ = 'connections'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    adapter = db.Column(db.String(30), nullable=False)
    conn_name = db.Column(db.String(100), nullable=False)
    # url = db.Column(db.String(200), nullable=False)
    host = db.Column(db.String(100), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    user = db.Column(db.String(200), nullable=False)
    password = db.Column(db.String(200), nullable=False)
    db_name = db.Column(db.String(200), nullable=False)
    notes = db.Column(db.String(200))
    debezium_config = db.Column(JSON)
    # user_connections = db.Relationship('User', backref='owner_connections')

    # Use a unique backref name like 'user_owned_connections'
    user_owned_connections = db.relationship('User', backref='connections')

    def __init__(self, adapter, conn_name, host, port, user, password, db_name, notes, debezium_config):
        self.adapter = adapter
        self.conn_name = conn_name
        self.host = host
        self.port = port
        self.user = user 
        self.password = password
        self.db_name = db_name
        self.notes = notes
        self.debezium_config = debezium_config

    def __repr__(self):
        return f"<Connection {self.name}>"


with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=3000)
