## Production version of code for Insight Data Engineers program.

# Overall strategy - define module "account" which contains a list of "contacts"
# Contacts are saved as Python dictionary, using account number as key and the "tier" level as value
# "tier" being the shorthand for the number of degrees of separation.
# Since Python dictionaries are extremely fast (equivalent to hash tables), should be able to flag transaction fast

# unfortunately, cannot get code to run on my system due to insufficient memory
# when it comes to calculating 3rd tier friends and beyond
# Have submitted with n=2 implemented only.

import pandas as pd
import csv
import numpy as np
from user_class import User, find_new_friends

## Paths to data files
input_dir = 'paymo_input/'
batch_file = 'batch_payment.csv'
stream_file = 'stream_payment.csv'
batch_path = input_dir + batch_file
stream_path = input_dir + stream_file


## Difficult to parse messages section because it's possible for it to use commas
## Below, figured out solution - import raw string, then use regular expressions to pick out each component
## we choose everything after the fourth comma to be the message
print('Loading data from batch_payment.csv')

batch_raw = []

with open(batch_path, newline='') as csvfile:
    for line in csvfile:
        batch_raw.append(line.strip())
        
df_batch = pd.DataFrame(batch_raw, columns = ['raw'])

## USE REGEX to parse out - everything before first comma is time, second comma is id1 and so on
## able to pick up messages with commas in them since it picks up all leftover characters at end of line
df_batch['time'] = df_batch['raw'].str.extract('^(.+?),', expand=True)
df_batch['id1'] = df_batch['raw'].str.extract('^[^,]*,([^,]*),', expand=True)
df_batch['id2'] = df_batch['raw'].str.extract('^[^,]*,[^,]*,([^,]*),', expand=True)
df_batch['amount'] = df_batch['raw'].str.extract('^[^,]*,[^,]*,[^,]*,([^,]*),', expand=True)
df_batch['message'] = df_batch['raw'].str.extract('^[^,]*,[^,]*,[^,]*,[^,]*,(.*$)', expand=True)

#can get rid of raw data now that we've parsed everything
df_batch = df_batch.drop('raw', 1)
df_batch = df_batch.drop(0, 0) #drop header row


#for convenience, let's call the id1 users 'givers' and id2 users 'receivers'
#strategy is to use the pandas groupby command to obtain list of all partners in transactions where <user_id> is giver
#repeat for transactions where <user_id> is receiver, then combine into single list
givers = df_batch.groupby('id1')
receivers = df_batch.groupby('id2')
partners_1 = {}
partners_2 = {}


print('Finding all transaction partners...')

## find all transactions where user <user_id> was giver, then find list of partners in those transactions
for user_id,transactions in givers:
    
    #store list of all transaction partners as list (easiest type to extend later)
    try:
        partners_1[int(user_id)] = list(givers.get_group(user_id)['id2'].astype(int))
   
    #some lines of batch_payment.txt and stream_payment.txt are off - omit malformed entries
    except (KeyError, ValueError) as BadLine:
        print("Skipping invalid key:",user_id)
    
## same as before for all transactions where <user_id> was receiver
for user_id,transactions in receivers:
    
    #store list of all transaction partners as list (easiest type to extend later)
    try: 
        partners_2[int(user_id)] = list(receivers.get_group(user_id)['id1'].astype(int))
        
    #some lines of batch_payment.txt and stream_payment.txt are off - omit malformed entries
    except (KeyError, ValueError) as BadLine:
        print("Skipping invalid key:",user_id)


## it's possible that some users only show up as givers and others only as receivers - combine to master list of all IDs
## in actuality for the provided batch_payment.txt all users show up as givers at least once, but not safe to assume
user_list_1 = np.array(list(partners_1.keys()))
user_list_2 = np.array(list(partners_2.keys()))
user_list = np.unique(np.concatenate([user_list_1,user_list_2]))


## important - following stores all of our information about each user
# stored as a dictionary of User types (defined in user_class.py)
user_master_list = {}

#cycle through all users and agglomerate partners from all transactions
#conversion back and forth between list and numpy array is pretty fast
#lists easier to append to, hence why stored as list, but also wanted to use numpy.unique function.
for user_id in user_list:
    
    pp = []
    
    if user_id in partners_1.keys():
        pp += partners_1[user_id]
        
    if user_id in partners_2.keys():
        pp += partners_2[user_id]
        
    #reduce to (sorted) list of all unique partners
    user_master_list[user_id] = User(user_id, list(np.unique(pp)))


## use the find_new_friends function (stored in user_class.py) to supplement friend tiers down to level of interest
#this is the most labor-intensive part of the program, unsurprisingly - n=2 takes 2 minutes on my macbook pro

#unfortunately, n=3 and n=4 are highly memory intensive...couldn't get them to work consistently
#change tier_depth to 3 or 4 for deeper friend networks...
tier_depth = 2

#successively add tiers of friendship to every user in user_master_list
for tier in range(2,tier_depth+1):
    
    print("Building lists of connections of degree", tier, "for each user...")
    for user_id, user_info in user_master_list.items():
        user_info.friends[tier] = find_new_friends(user_master_list,user_info,tier)
        
print("Done. Connections of degree n accessible via User.friends[n]")


## create forward lookup ##
## for each user, dictionary dos links a user id with the level of friendship
for user_id, user_data in user_master_list.items():
    user_data.build_dos()

## DEFINE TESTS ##

## the following tests take in pairs of user IDs as strings, convert them to int and use
## User.dos to look up whether they are connected to each other, and if so at what degree.

#first test - have these people had a transaction with each other in the batch data set?
def test1(id1,id2):
    
    if type(id1)==str and type(id2)==str:
    
        #converts user IDs from string into ints, which we use as dictionary keys
        try:
            user1 = int(id1)
        except ValueError:
            return 'unverified'

        try:
            user2 = int(id2)
        except ValueError:
            return 'unverified'
        
        #this being python, the following line doesn't take up new memory - just a shorthand
        if user1 in user_master_list.keys(): #have to check in case id1 is a new user
            user_dos = user_master_list[user1].dos

            if user2 in user_dos.keys():
                if user_dos[user2] == 1:
                    return 'trusted'
    
    return 'unverified'

#second test - is the transaction partner either a 1st or 2nd degree connection?    
def test2(id1,id2):
    
    if type(id1)==str and type(id2)==str:
    
        #converts user IDs from string into ints, which we use as dictionary keys
        try:
            user1 = int(id1)
        except ValueError:
            return 'unverified'

        try:
            user2 = int(id2)
        except ValueError:
            return 'unverified'
    
        #this being python, the following line doesn't take up new memory - just a shorthand
        if user1 in user_master_list.keys(): #have to check in case id1 is a new user

            user_dos = user_master_list[user1].dos

            if user2 in user_dos.keys():
                if user_dos[user2] in range (1,3):
                    return 'trusted'
    
    return 'unverified'
    
#third test - is the transaction partner at least a 4th degree connection?
#even though fourth degree networks weren't successfully implemented yet,
#the program has been designed to work if they are    
def test3(id1,id2):
    
    if type(id1)==str and type(id2)==str:
    
        #converts user IDs from string into ints, which we use as dictionary keys
        try:
            user1 = int(id1)
        except ValueError:
            return 'unverified'

        try:
            user2 = int(id2)
        except ValueError:
            return 'unverified'
        
        #this being python, the following line doesn't take up new memory - just a shorthand
        if user1 in user_master_list.keys(): #have to check in case id1 is a new user    

            user_dos = user_master_list[user1].dos

            if user2 in user_dos.keys():
                if user_dos[user2] in range(1,5):
                    return 'trusted'

    return 'unverified'

## Now load in second data set - this time we choose to flag transactions as verified or unverified.
## again, using regular expressions to parse from raw data.
print('Loading stream_payment.csv into Pandas...')

stream_raw = []

with open(stream_path, newline='') as csvfile:
    for line in csvfile:
        stream_raw.append(line.strip())
        
df_stream = pd.DataFrame(stream_raw, columns = ['raw'])

## USE REGEX to parse out - everything before first comma is time, second comma is id1 and so on
## able to pick up messages with commas in them since it picks up all leftover characters at end of line
df_stream['time'] = df_stream['raw'].str.extract('^(.+?),', expand=True)
df_stream['id1'] = df_stream['raw'].str.extract('^[^,]*,([^,]*),', expand=True)
df_stream['id2'] = df_stream['raw'].str.extract('^[^,]*,[^,]*,([^,]*),', expand=True)
df_stream['amount'] = df_stream['raw'].str.extract('^[^,]*,[^,]*,[^,]*,([^,]*),', expand=True)
df_stream['message'] = df_stream['raw'].str.extract('^[^,]*,[^,]*,[^,]*,[^,]*,(.*$)', expand=True)

#can get rid of raw data now that we've parsed everything
df_stream = df_stream.drop('raw', 1)
df_stream = df_stream.drop(0, 0) #drop header row


## create output columns
## I found that pandas started to slow down drastically when trying to calculate over 1,000,000
## entries at a time...hence broken into pieces

#test 1 - immediate adjacency
test1_a = df_stream[:1000000].apply(lambda x: test1(x['id1'], x['id2']), axis=1)
test1_b = df_stream[1000000:2000000].apply(lambda x: test1(x['id1'], x['id2']), axis=1)
test1_c = df_stream[2000000:].apply(lambda x: test1(x['id1'], x['id2']), axis=1)
df_stream['test1'] = pd.concat([test1_a, test1_b, test1_c])

print('Test 1 results:')
print(df_stream['test1'].value_counts())

#test 2 - 2nd degree friends pass as verified
test2_a = df_stream[:1000000].apply(lambda x: test2(x['id1'], x['id2']), axis=1)
test2_b = df_stream[1000000:2000000].apply(lambda x: test2(x['id1'], x['id2']), axis=1)
test2_c = df_stream[2000000:].apply(lambda x: test2(x['id1'], x['id2']), axis=1)
df_stream['test2'] = pd.concat([test2_a, test2_b, test2_c])

print('Test 2 results:')
print(df_stream['test2'].value_counts())

#test 3
test3_a = df_stream[:1000000].apply(lambda x: test3(x['id1'], x['id2']), axis=1)
test3_b = df_stream[1000000:2000000].apply(lambda x: test3(x['id1'], x['id2']), axis=1)
test3_c = df_stream[2000000:].apply(lambda x: test3(x['id1'], x['id2']), axis=1)
df_stream['test3'] = pd.concat([test3_a, test3_b, test3_c])

print('Test 3 results:')
print(df_stream['test3'].value_counts())


## OUTPUT TO TEXT FILE ## 
input_dir = 'paymo_output/'
file_1 = input_dir + 'output1.txt'
file_2 = input_dir + 'output2.txt'
file_3 = input_dir + 'output3.txt'

## overwrites whatever is in the file by default
df_stream['test1'].to_csv(file_1, index=False)
df_stream['test2'].to_csv(file_2, index=False)
df_stream['test3'].to_csv(file_3, index=False)

print('Saved output successfully to paymo_output')