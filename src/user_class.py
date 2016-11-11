#Included as part of Jesse Day's solution to digital-wallet, November 10th, 2016.

#Given a particular user, two things are useful to keep track of:
#1) A list of all other users who are nth-degree acquaintances;
#2) A forward lookup using a dictionary where we provide user id number and see what tier of acquaintance they are.

#both are implemented here: the former as the dictionary of lists 'acquaintances', and the latter as a dictionary called 'degrees_of_separation'

#The initial parameters we take in are the user_id, a list of partners, and also how many tiers of acquaintance we want to 
#calculate right off the bat.

import numpy as np
import time

class User:
    
    def __init__(self, user_id, partners):
        
        self.user_id = user_id
        self.friends = {}
        self.friends[1] = partners #a dictionary of lists, with the key value reflecting nth tier of separation.
        self.dos = {} #dos = degrees_of_separation. key is user id, output is how far removed that friend is
        
    def build_dos(self):
        for tier, friend_list in self.friends.items():
            for friend in friend_list:
                self.dos[friend] = tier
        
        
## find_new_friends function takes in a big dictionary of users (i.e. user_master_list), a current user of interest.
## the tier parameter indicates what degree of friendship we're searching for - need it so we know what existing friends
#to account for.
def find_new_friends(user_list, current_user, new_tier):
    
    #sanity check in case we incorrectly specify new_tier somewhere else
    if new_tier <= 1:
        print('Function only works if there are at least some pre-existing friends (choose higher tier)')
        return
    
    #check if there are any friends in the preceding tier - otherwise, may be filling in wrong order
    if not current_user.friends[new_tier-1]:
        print('No friends in previous tier. Have you filled previous tier of friends yet?')
        return
    
    #we have to pool together all existing first-degree friends, second-degree, etc.
    #by initializing existing_friends with the user themselves, avoid ever adding them to new_friends
    existing_friends = [current_user.user_id] 
    
    #don't need to check for redundancy of users in tiers - any given user can only belong to one tier
    for key in range(1,new_tier):
        existing_friends += current_user.friends[key]
    
    tentative_new = []
    
    #for each user who is currently a friend of degree new_tier-1, add all of their first-degree friends to tentative_new
    for friend_id in current_user.friends[new_tier-1]:
        tentative_new += user_list[friend_id].friends[1] 
    
    tentative_new = list(np.unique(tentative_new)) #get rid of duplicates
    #following line returns 
    new_friends = [ x for x in tentative_new if x not in existing_friends ]
    
    return new_friends

