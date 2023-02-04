# Amanda Hanway - Streaming Data, Module 5
- Date: 2/4/23

# streaming-05-smart-smoker

## Program Overview:
- Bbq_producer.py
    - This program gets data from a csv file then sends each column as a message to a different queue on the RabbitMQ server   
- CSV Data Source
    - The smoker-temps.csv file contains 4 columns of data from a smart bbq smoker:
        - Time = Date-time stamp for the sensor reading
        - Channel1 = Smoker Temp 
        - Channe2 = Food A Temp 
        - Channe3 = Food B Temp 

## Instructions:
- Requires RabbitMQ server to be running and pika to be installed in your active environment
- Bbq_producer.py
    - Set constant for host name
    - Turn on (show_offer=true) or turn off (show_offer=false) asking the user if they'd like to open the RabbitMQ Admin site 

## Project Requirements - Smart Smoker Description:
- Using a Barbeque Smoker
    - When running a barbeque smoker, we monitor the temperatures of the smoker and the food to ensure everything turns out tasty. Over long cooks, the following events can happen:
        - The smoker temperature can suddenly decline.
        - The food temperature doesn't change. At some point, the food will hit a temperature where moisture evaporates. It will stay close to this temperature for an extended period of time while the moisture evaporates (much like humans sweat to regulate temperature). We say the temperature has stalled.
 
- Sensors
    - We  have temperature sensors track temperatures and record them to generate a history of both (a) the smoker and (b) the food over time. These readings are an example of time-series data, and are considered streaming data or data in motion.

- Streaming Data
    - Our thermometer records three temperatures every thirty seconds (two readings every minute). The three temperatures are:
        - the temperature of the smoker itself.
        - the temperature of the first of two foods, Food A.
        - the temperature for the second of two foods, Food B.

- Significant Events
    - We want know if:
        - The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
        - Any food temperature changes less than 1 degree F in 10 minutes (food stall!)

- Smart System
    - We will use Python to:
        - Simulate a streaming series of temperature readings from our smart smoker and two foods.
        - Create a producer to send these temperature readings to RabbitMQ.
        - Create three consumer processes, each one monitoring one of the temperature streams. 
        - Perform calculations to determine if a significant event has occurred.

- Optional: Alert Notifications
    - Optionally, we can have our consumers send us an email or a text when a significant event occurs. 
    - You'll need some way to send outgoing emails. 

## Screenshot - Producer Program Running in Terminal:

![Program Running](Producer_running_screenshot.png)