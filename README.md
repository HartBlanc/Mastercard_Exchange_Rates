# Mastercard_Exchange_Rates

Scrapes all Mastercard exchange rates from 1 year ago to the present day to an SQLite Database

The code could be easily modified to retrieve other data or store in different formats.




## Some information about the exchange rate data:

  Some exchange rates are not available before 14th October 2016
  
  No exchange rates are available for saturday and sunday before 14th October 2016
  
  The rates for the present day become available at 2pm EST.




## Some information about the scripts:
I have been running this script in python 3.6, and have not included any attempts at backwards compatability.
One exchange rate is recieved per request.
  
### rate_retriever.py
Retrieves all rates from 1 year ago to the present day with a single thread.
It searchs by first looking to see if the rates are available only from October 14th, then the 1st day (where most rates are clustered) then does a binary search within the range. To retrieve all the data this program would be incredibly slow, 30+ days.
  
### rate_retriever_multi.py
Is effectively rate_retriever.py with multiple threads. However, because of the way multiprocessing.pool.starmap works the requests has to wait for all data to be retrieved. Because of the variation in start dates this is a severe bottleneck and the program will still take a very long time to retrieve large volumes of data, although this is the best option if you want data over a long range for a small number of currencies.
The multiple threads work by breaking the data into chunks of equal size and then processing them individually.  
 
 
### rate_retriever_multi_oct_14.py
Retrieves all rates from October 14th to the present day with multiple threads. This reduces the bottleneck problem substantially and is probably the only viable option for donwloading data from all currencies.
  
## Potential improvements include:

Any suggestions on how to relieve the bottleneck issue would be greatly appreciated, this is my first attempt at using multiprocessing.

Versions compatible with earlier versions of python.

An additional script that evaluates peformance at different thread counts to suggest the optimal level.

(my preliminary tests on my 4-core i5 macbook did well at 15 threads)
  
  
