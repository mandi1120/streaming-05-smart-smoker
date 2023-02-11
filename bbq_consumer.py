"""
    Amanda Hanway - Streaming Data, Module 5
    2/4/23

    This program listens for messages from three queues 
    on the RabbitMQ server continuously.
    It performs transformations on messages when received
    and generates an alert message when specific events occur.

    Author: Amanda Hanway 
    Date: 2/4/23
"""

######## imports ########
import pika
import sys
import time
import csv
from collections import deque


######## declare constants ########
# set host and queue name
host = "localhost"
smoker_temp_queue_name = "01-smoker"
food_a_queue_name = "02-food-A"
food_b_queue_name = "03-food-B"

# Create empty deques to store that last n messages
smoker_deque = deque(maxlen=5)
food_a_deque = deque(maxlen=20)
food_b_deque = deque(maxlen=20)


######## define functions ########
def smoker_callback(ch, method, properties, body):
    """ 
    Define behavior on getting a Smoker Temp mmessage.
    """
    # decode the binary message body to a string
    print(f" [x] Received: [Smoker]  {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # removed, acknowledging automatically (auto_ack=True)
    # ch.basic_ack(delivery_tag=method.delivery_tag)

    # add the message to a deque
    smoker_deque.append(body.decode())

    # compare this temp with the temp from 4 reading ago
    # alert if smoker temperature decreases by 15+ degrees F in 2.5 minutes (or 5 readings)
    smoker_current = body.decode()
    smoker_current_sp = smoker_current.split(", ") #split the string into time/temp
    smoker_current_temp = float("0.0" if smoker_current_sp[1] == 'Channel1' or smoker_current_sp[1] == '' else smoker_current_sp[1]) 
    
    if smoker_current_temp > 1:
        smoker_compare = smoker_deque[0] #first item in deque, 5 readings ago
        smoker_compare_sp = smoker_compare.split(", ") #split the string into time/temp
        smoker_compare_temp = float("0.0" if smoker_compare_sp[1] == 'Channel1' or smoker_compare_sp[1] == '' else smoker_compare_sp[1])
        if smoker_compare_temp > 1 and smoker_compare_temp - smoker_current_temp >= 15:
            print(f" >>>>> Smoker alert! Temperature decreased by 15 or more degrees in 2.5 minutes. {smoker_compare_temp} to {smoker_current_temp}")

def food_a_callback(ch, method, properties, body):
    """ 
    Define behavior on getting a Food A message.
    """
    # decode the binary message body to a string
    print(f" [x] Received: [Food A]  {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # removed, acknowledging automatically (auto_ack=True)
    # ch.basic_ack(delivery_tag=method.delivery_tag)

    # add the message to a deque
    food_a_deque.append(body.decode())

    # compare this temp with the temp from 19 reading ago
    # alert if food temperature changed less than 1 degree F in 10 minutes (or 20 readings)
    # note this does not indicate hotter vs cooler, so using the absolute value of change
    food_a_current = body.decode()
    food_a_current_sp = food_a_current.split(", ") #split the string into time/temp
    food_a_current_temp = float("0.0" if food_a_current_sp[1] == 'Channel2' or food_a_current_sp[1] == '' else food_a_current_sp[1]) 

    if food_a_current_temp > 1:
        food_a_compare = food_a_deque[0] #first item in deque, 20 readings ago
        food_a_compare_sp = food_a_compare.split(", ") #split the string into time/temp
        food_a_compare_temp = float("0.0" if food_a_compare_sp[1] == 'Channel2' or food_a_compare_sp[1] == '' else food_a_compare_sp[1])
        if food_a_compare_temp > 1 and abs(food_a_current_temp - food_a_compare_temp) <= 1:
            print(f" >>>>> Food stall alert! Food A temperature changed by 1 degree or less in 10 minutes. {food_a_compare_temp} to {food_a_current_temp}")

def food_b_callback(ch, method, properties, body):
    """ 
    Define behavior on getting a Food B message.
    """
    # decode the binary message body to a string
    print(f" [x] Received: [Food B]  {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # removed, acknowledging automatically (auto_ack=True)
    # ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # add the message to a deque
    food_b_deque.append(body.decode())

    # compare this temp with the temp from 19 reading ago
    # alert if food temperature changed less than 1 degree F in 10 minutes (or 20 readings)
    # note this does not indicate hotter vs cooler, so using the absolute value of change
    food_b_current = body.decode()
    food_b_current_sp = food_b_current.split(", ")
    food_b_current_temp = float("0.0" if food_b_current_sp[1] == 'Channel3' or food_b_current_sp[1] == '' else food_b_current_sp[1])    

    if food_b_current_temp > 1:
        food_b_compare = food_b_deque[0] #first item in deque, 20 readings ago
        food_b_compare_sp = food_b_compare.split(", ") #split the string into time/temp
        food_b_compare_temp = float("0.0" if food_b_compare_sp[1] == 'Channel3' or food_b_compare_sp[1] == '' else food_b_compare_sp[1])        
        if food_b_compare_temp > 1 and abs(food_b_current_temp - food_b_compare_temp) <= 1:
            print(f" >>>>> Food stall alert! Food B temperature changed by 1 degree or less in 10 minutes. {food_b_compare_temp} to {food_b_current_temp}")

# define a main function to run the program
def main(hn: str = host, qn1: str = smoker_temp_queue_name, qn2: str = food_a_queue_name, qn3: str = food_b_queue_name):
    """ 
    Continuously listen for task messages on a named queue.
    Parameters:
    host (str): the host name or IP address of the RabbitMQ server
    smoker_temp_queue_name (str): the name of the smoker queue
    food_a_queue_name (str): the name of the food A queue
    food_b_queue_name (str): the name of the food B queue
    """   
    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()
        
        # do this once for each queue
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn1, durable=True)
        channel.queue_declare(queue=qn2, durable=True)
        channel.queue_declare(queue=qn3, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # do this once for each queue
        # configure the channel to listen on a specific queue,  
        # use the callback function for the associated queue/channel,
        # and auto-acknowledge the message  
        channel.basic_consume(queue=qn1, on_message_callback=smoker_callback, auto_ack=True)
        channel.basic_consume(queue=qn2, on_message_callback=food_a_callback, auto_ack=True)
        channel.basic_consume(queue=qn3, on_message_callback=food_b_callback, auto_ack=True)                

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":

    main()

    #main(host, '0', '0', food_b_queue_name)

