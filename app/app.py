#!/usr/bin/env python3
from flask import Flask
from flask import render_template, request, redirect, url_for, session, flash 
from redis import Redis
from rq import Worker, Queue, Connection
#Postgres
import os
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import bcrypt

#from flask_login import login_user, login_required, logout_user, LoginManager, UserMixin, current_user
from flask_login import UserMixin, login_user, LoginManager, login_required, logout_user, current_user
from sqlalchemy.dialects.postgresql import JSON

login_manager = LoginManager()

app = Flask(__name__)

# Secret Key
app.secret_key = 'base64_32_digit_key'

# DB config
app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:postgres@localhost:5432/metasync_development'
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Flask Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'users/registration'

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
        return render_template('dashboard.html')
    else:
        return render_template('users/new.html')

@app.route('/users/<int:user_id>', methods=['GET', 'POST'])
@login_required
def dashboard(user_id):
    
    connections = Connection.query.filter_by(user_id=current_user.id).all()

    return render_template('dashboard.html', connections=connections)

@app.route('/users/<int:user_id>/connections/new', methods=['GET', 'POST'])
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

        
        cuser_id = current_user.id
        new_connection = Connection(host=host,port=port, user=user, password=password, adapter=adapter, db_name=db_name, notes=notes)
        new_connection.user_id = cuser_id
        db.session.add(new_connection)
        db.session.commit()
        return redirect(url_for('dashboard', user_id = current_user.id))
    else:
        return render_template('connection/new.html')




@app.route('/users/<int:user_id>/connections/realtime/new', methods=['GET', 'POST'])
@login_required
def add_realtime_connection(user_id):
    return render_template('connection/new.html')

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

# Integration connections
# Onect to many relationship

# once connected, solve the 4 problems

# Use another service, kafka for replication and apache spark for real time
# DOnt run SQL queries direct on DB for batch process, integrate S3 for that
# for real time look into apache kafka
# add ability to delete connections too.

class Connection(db.Model, UserMixin):
    __tablename__ = 'connections'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    adapter = db.Column(db.String(30), nullable=False)
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

    def __init__(self, adapter, host, port, user, password, db_name, notes, debezium_config):
        self.adapter = adapter 
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
