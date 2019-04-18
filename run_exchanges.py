import argparse
from sqlalchemy.orm import sessionmaker
import time
from db_connect import get_conn
from random import randint

## Argument parser to take the parameters from the command line
## Example on how to run: python run_exchanges.py 10 READ_COMMITTED
parser = argparse.ArgumentParser()
parser.add_argument('E', type = int, help = 'number of swaps')
parser.add_argument('I', help = 'isolation level')
args = parser.parse_args()

## Exchange transaction swaps the balance of two accounts
def exchange(sess):
    ## 0. Generate 2 random account numbers - 2 ids
    rand_id1 = randint(1,100000)
    rand_id2 = randint(1,100000)
    while rand_id1==rand_id2:
        rand_id2 = randint(1,100000)
        #print("Inside while")
    ## 1. Read the balance from first account A1(id1,branch1) into v1
    res = sess.execute("select balance from account where id=%d;" % (rand_id1))
    v1 = res.fetchone()['balance']
    #print("Balance in Account id  ",rand_id1," is ",v1)

    ## 2. Read the balance from first account A1(id2,branch2) into v2
    res = sess.execute("select balance from account where id=%d;" % (rand_id2))
    v2 = res.fetchone()['balance']
    #print("Balance in Account2 id  ",rand_id2," is ",v2)

    ## 3. Write value v1 into account A2
    res = sess.execute("update ACCOUNT set balance=%f where id=%d;" % (v1,rand_id2))

    ## 4. Write value v2 into account A1
    res = sess.execute("update ACCOUNT set balance=%f where id=%d;" % (v2,rand_id1))

    sess.commit()
    ## END of exchange function

## Create E swaps operations
def E_swaps(sess, E):
    #sums = []
    start = time.time()
    deadlock_count = 0
    for i in xrange(0, E):
        #while True:
        try:
            exchange(sess)
            #sums.append(sum)
        except Exception as e:
            #print e
	    deadlock_count += 1
            continue
        time.sleep(0.0001)

    stop = time.time()
    return deadlock_count,stop-start

## Create the engine and run the sums
engine = get_conn()
Session = sessionmaker(bind=engine.execution_options(isolation_level=args.I, autocommit=True))
sess = Session()
deadlock_count, time = E_swaps(sess, args.E)
#print "time",time
#print "dead",deadlock_count
print time*(args.E)/(args.E - deadlock_count)
