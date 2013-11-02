# -*- encoding: utf8 -*-

import datetime
import time
import msgpack
import zmq
context = zmq.Context()
sock    = context.socket(zmq.REQ)
sock.connect("tcp://localhost:5555")


def send(sock, db, cmd, *args, **kwargs):
  #sock.send_unicode(cmd.encode('utf-8'))
  #sock.send(msgpack.packb([ db.encode('utf-8'), 1]))
  sock.send(msgpack.packb(["1", "db"]))


def recv(sock, **kwargs):
  msg = sock.recv()
  print "recv: ", msg, kwargs
  try:
    msgpack.unpackb(msg)
  except Exception:
    "Error: invalid msgpack"

DB = 'datomic:mem://db1'


SCHEMA = """
{:db/id #db/id[:db.part/db]
 :db/ident :person/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/doc "A person's name"
 :db.install/_attribute :db.part/db}
{:db/id #db/id[:db.part/db]
 :db/ident :person/email
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/doc "A person's email"
 :db/index true
 :db.install/_attribute :db.part/db}
"""

PERSON = """
[{:db/id #db/id[:db.part/user]
  :person/name "Bob"
  :person/email "bob@example.com"}]
"""


iters = 100
a = datetime.datetime.now()
for request in range (iters):
    
  # create database
  print 'send x'
  sock.send(msgpack.packb(["1", DB, SCHEMA]))
  recv(sock, debug=request)

  """
  # schema
  sock.send(msgpack.packb(["2", DB, SCHEMA]))
  recv(sock, debug=request)

  # transact
  sock.send(msgpack.packb(["2", DB, PERSON]))
  recv(sock, debug=request)
  """

  time.sleep( 5 )


b = datetime.datetime.now()


def bench(aaa,bbb,i,s, misc=None):
  d = bbb - aaa
  microsecs = d.microseconds + float(d.seconds * 1000000)
  sec = float(microsecs) / 1000000.0
  ms  = microsecs / 1000.0
  msi = ms / float(i)
  usi = msi * 1000
  rps = 1000000.0 / usi
  print ""
  print "-------:: %s :: ---------" % s.upper()
  print ""
  print u"{} loops in {:,.3f}ms /  {:,.0f}us".format(i, ms, microsecs)
  print u"{:,.0f} per second. \n 1 loop every {:,.3f}ms \n {:,.0f}us per iter. ".format(rps, msi, usi)


bench(a,b,iters,'zmq')
