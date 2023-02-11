"""
    Amanda Hanway - Streaming Data, Module 5
    2/4/23
 
    This program gets data from a csv file 
    then sends each column as a message to a 
    different queue on the RabbitMQ server.

    Author: Amanda Hanway 
    Date: 2/4/23
"""

######## imports ########
import pika
import sys
import webbrowser
import csv
import time


######## declare constants ########
# set host and queue name
host = "localhost"
smoker_temp_queue_name = "01-smoker"
food_a_queue_name = "02-food-A"
food_b_queue_name = "03-food-B"

# set input file 
csv_file = 'smoker-temps.csv'

# set to turn on (true) or turn off (false) asking the 
# user if they'd like to open the RabbitMQ Admin site 
show_offer = True


######## define functions ########
def offer_rabbitmq_admin_site(show_offer):
    """
    Offer to open the RabbitMQ Admin website
    """
    if show_offer == True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def delete_queue(host: str, queue_name: str):    
    """
    Delete a queue to clear un-needed messages
    if the code had been run previously

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
    """
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    # delete the queue
    ch.queue_delete(queue=queue_name)    

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:       
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def get_and_send_message_from_csv(input_file):
    """
    Open the csv input file and read each row as a message
    then send the message to the queue
    """
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter=",")      
        for row in reader:

            # get the "timestamp" column 
            fstring_message_timestamp = f"{row[0]}"

            # get the "smoker temp" column and create a message  
            fstring_message_smoker = f"{fstring_message_timestamp}, {row[1]}"
            # prepare a binary (1s and 0s) message to stream
            smoker_message = fstring_message_smoker.encode()
            # send the message
            send_message(host, smoker_temp_queue_name, smoker_message)

            # get the "food a temp" column and create a message  
            fstring_message_food_a = f"{fstring_message_timestamp}, {row[2]}"
            # prepare a binary (1s and 0s) message to stream
            food_a_message = fstring_message_food_a.encode()   
            # send the message
            send_message(host, food_a_queue_name, food_a_message)

            # get the "food b temp" column and create a message  
            fstring_message_food_b = f"{fstring_message_timestamp}, {row[3]}"
            # prepare a binary (1s and 0s) message to stream
            food_b_message = fstring_message_food_b.encode()   
            # send the message
            send_message(host, food_b_queue_name, food_b_message)

            # read one value every half minute 
            time.sleep(30)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # if show_offer is turned on (True) then
    # ask the user if they'd like to open the RabbitMQ Admin site 
    offer_rabbitmq_admin_site(show_offer) 

    # delete the queue if run previously
    delete_queue(host, smoker_temp_queue_name)
    delete_queue(host, food_a_queue_name)
    delete_queue(host, food_b_queue_name)

    # get the message from an input file
    # send the message to the queue
    get_and_send_message_from_csv(csv_file)

