# WeblogChallenge

This is my solution for the WeblogChallenge.

## Instructions to reproduce the results

My machine has Ubuntu 19.04. I used Spark 2.4.3.
Prerequisites - sbt installed and spark-submit is in path.
For running the project, go to the root of project and execute below commands:

sbt package

spark-submit --class paytm.weblogChallenge target/scala-2.11/weblogchallenge_2.11-1.0.jar data/2015_07_22_mktplace_shop_web_log_sample.log.gz 30 results/


It will output some of the results in the terminal and some in the results directory.

## Data Cleaning

During exploratory analysis, I observed that few "backend_port" & "user_agent" values are not present.

They have "-" as values. This means that a session is not complete.

Also, sometimes column has null values  so not selecting rows where important information is missing
which is needed for sessioning the data.

Therefore, cleaning the data where it has "-" in "backend_port" & "user_agent" and selecting not null from
"timestamp", "client_port", "request" & "user_agent".

Further data cleaning by extracting client ip & url from "client_port" & "request" respectfully.

Converting timestamp to epoch seconds and extracting date which will help to establish sessions later.

## Findings

Challenges:

#### Challenge 1. Sessionize the web log

I used time-based expiration to sessionize. I find it difficult to sessionize using time based window.

Because time window for a session can vary from users to users. 

Rather it is a convention that 30 minutes of inactivity can be considered as a new session.

Sessions (using time-based expiration) can be broadly determined using following 2 criteria:
   1. More than 30 minutes of inactivity
   2. At midnight (date change)
   
Using window function (in the code) and with the two time-based expiration criteria, I have calculated sessions.

These sessions have been numbered using the same window.

Then an unique session id is assigned to them using a hash of "client_ip", "user_agent" & "session_num".

Data with session id is saved in "results/weblog_data_sessionized" as csv file.

#### Challenge 2. Determine the average session time

Session duration for each session is calculated by grouping using client_ip, user_agent & session_id (established above) and calculate .

Also, removing sessions which is zero in duration. These sessions has only 1 activity.

Then, average session duration is calculated by taking average of all sessions' duration.

Average session duration in seconds is as follows:

705.8640270071671

#### Challenge 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

First, grouping using client_ip, user_agent & session_id (established above) and then counted distinct urls for each session.

Distinct urls for each session is saved in "results/weblog_data_unique_urls" as csv file.

A sample of unique URL visits per session:

+------------+----------------------+

|client_ip   |unique_url_per_session|

+------------+----------------------+

|1.186.76.11 |13                    |

|1.22.132.104|19                    |

|1.22.187.226|1                     |

|1.22.84.151 |8                     |

|1.23.80.25  |6                     |

|1.38.16.236 |3                     |

|1.38.20.50  |2                     |

|1.39.10.140 |1                     |

|1.39.12.167 |3                     |

|1.39.12.227 |3                     |

+------------+----------------------+


#### Challenge 4. Find the most engaged users, ie the IPs with the longest session times

Calculated difference between session duration across sessions using reduce.

The most engaged user is as follows:

client_ip = 119.81.61.166

user_agent = Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10

session_duration = 3039

## Challenge Description
This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

A. Fork this repo in github
    https://github.com/PaytmLabs/WeblogChallenge

B. Complete the processing and analytics as defined first to the best of your ability with the time provided.

C. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the Paytm Labs interview team.

D. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
