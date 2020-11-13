from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext
import logging  
import json
from datetime import datetime
import pandas as pd
import numpy as np

TYPE = "scand"

if TYPE == "ukus":
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy']
    bonus_value = 35
    numberOfDice = 5
elif TYPE == "scand":
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'onePair', 'twoPairs', 'threeOfAKind', 'fourOfAKind', 'smallStraight', 'largeStraight', 'fullHouse', 'chance', 'yatzy']
    bonus_value = 50   
    numberOfDice = 5
else:
    numberOfDice = 6
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'onePair', 'twoPairs', 'threePairs', 'threeOfAKind', 'fourOfAKind', 'fiveOfAKind', 'smallStraight', 'largeStraight', 'fullStraight', 'fullHouse', 'threePlusThree', 'fourPlusTwo', 'chance', 'yatzym']
    bonus_value = 100

col_names = ["category_chosen"]
for k in range(numberOfDice):
    col_names.append("dice_value_" + str(k))
for k in categories:
    col_names.append(str(k))
col_names.append("final_score")
col_names.append("possible_early_finish")
print("col_names")
print(col_names)

def add_features_ukus( x ):
    # ['category_chosen', 'dice_value_0', 'dice_value_1', 'dice_value_2', 'dice_value_3', 'ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy', 'final_score', 'possible_early_finish']
    global col_names
    global numberOfDice
    global categories
    global TYPE
    x = list(x)
    print(x)
    # x[ int(x[0])+6 ] = -1
    # x[ int(x[col_names.index("category_chosen")])+6 ] = -1
    
    # dices = np.array( x[1:6] )
    dices = np.array( x[col_names.index("dice_value_0"):col_names.index("ones")] )
    
    
    cats = np.array( x[6:19] )
    cats = np.array( x[col_names.index("ones"):col_names.index("final_score")] )
    cats[ cats>=0 ] = 1
    cats[ cats<0 ] = 0

    # cat_values_for_the_given_dices = np.zeros( (13,) ) 
    cat_values_for_the_given_dices = np.zeros( (len(categories),) ) 
    for i in range(numberOfDice + 1):
        cat_values_for_the_given_dices[i] = np.sum( dices[dices==(i+1)] )

    dice_counts = np.histogram( dices , bins=range(1,8)) [0]
    print("dice_counts")
    print(dice_counts)
    
    # One pair, Two pairs and Three pairs
    if TYPE == "scand" or TYPE == "scand6":
        count_pairs = np.where( dice_counts>=2 )[0]
        if len(count_pairs)>=2:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_pairs[-1]+1 )
            cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_pairs[-1]+1 ) + 2*( count_pairs[-2]+1 )
        if len(count_pairs)>=1:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_pairs[-1]+1 )
        if TYPE == "scand6":
            if len(count_pairs)==3:
                cat_values_for_the_given_dices[categories.index("threePairs")] = 2*( count_pairs[-1]+1 ) + 2*( count_pairs[-2]+1 ) + 2*( count_pairs[-3]+1 )
            count_three_plus_three = np.where( dice_counts>=3 )[0]
            if len(count_three_plus_three) == 2:
                cat_values_for_the_given_dices[categories.index("threePlusThree")] = 3*( count_three_plus_three[-1]+1 ) + 3*( count_three_plus_three[-2]+1 )
            count_fiveOfAKind = np.where( dice_counts==5 )[0]
            if len(count_fiveOfAKind) == 1:
                cat_values_for_the_given_dices[categories.index("fiveOfAKind")] = 5*( count_fiveOfAKind[-1]+1 )


    count_three_ind = np.where( dice_counts>=3 )[0]
    if len(count_three_ind)==1:
        cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_three_ind[0]+1 )
        
        count_two_ind = np.where( dice_counts==2 )[0]
        if len(count_two_ind)==1:
            cat_values_for_the_given_dices[categories.index("fullHouse")] = 3*( count_three_ind[0]+1 ) + 2*( count_two_ind[0]+1 )  

    count_four_ind = np.where( dice_counts>=4 )[0]
    if len(count_four_ind)==1:
        cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_four_ind[0]+1 )
        if TYPE == "scand6":
            count_rest_two = np.where( dice_counts==2 )[0]
            if len(count_rest_two) > 0:
                cat_values_for_the_given_dices[categories.index("fourPlusTwo")] = 4*( count_four_ind[0]+1 ) + 2*( count_rest_two[0]+1 ) 
                


    # Small straight, large straight and full straight for all categories     
    if TYPE == "ukus":
        dice_counts_p = np.copy(dice_counts)
        dice_counts_p[ dice_counts_p>0 ] = 1
        if (np.sum( dice_counts_p==np.array( [1,1,1,1,0,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,1,1,1,1,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,0,1,1,1,1] ) )==6)  or (np.sum( dice_counts_p==np.array( [1,0,1,1,1,1] ) )==6) or (np.sum( dice_counts_p==np.array( [1,1,1,1,0,1] ) )==6):
            cat_values_for_the_given_dices[categories.index("smallStraightu")] = 30
            
        if (np.sum( dice_counts==np.array( [0,1,1,1,1,1] ) )==6) or (np.sum( dice_counts==np.array( [1,1,1,1,1,0] ) )==6):
            cat_values_for_the_given_dices[categories.index("largeStraightu")] = 40
            cat_values_for_the_given_dices[categories.index("smallStraightu")] = 30
    
    count_different = np.where( dice_counts>=1 )[0]
    if TYPE == "scand":
        if len(count_different) == 5:
            cat_values_for_the_given_dices[categories.index("largeStraight")] = 20
            cat_values_for_the_given_dices[categories.index("smallStraight")] = 15
    if TYPE == "scand6":
        if len(count_different) >= 5 and dice_counts[categories.index("twos")] > 0 and dice_counts[categories.index("threes")] > 0 and dice_counts[categories.index("fours")] > 0 and dice_counts[categories.index("fives")] > 0:
            cat_values_for_the_given_dices[categories.index("largeStraight")] = 20
            cat_values_for_the_given_dices[categories.index("smallStraight")] = 15
        if len(count_different) == 6:
            cat_values_for_the_given_dices[categories.index("fullStraight")] = 21


        
    cat_values_for_the_given_dices[categories.index("chance")] = np.sum( dices )
    
    if TYPE == "ukus" or TYPE == "scand":
        count_five_ind = np.where( dice_counts==5 )[0]
        if len(count_five_ind)==1:
            cat_values_for_the_given_dices[categories.index("yatzy")] = 50    
    if TYPE == "scand6":
        count_six_ind = np.where( dice_counts==6 )[0]
        if len(count_six_ind)==1:
            cat_values_for_the_given_dices[categories.index("yatzym")] = 100
    
    cat_values_for_the_given_dices = cat_values_for_the_given_dices*(1-cats) 

    # upper = np.array( x[6:12] )
    upper = np.array( x[col_names.index("ones"):(col_names.index("sixes")+1)] )

    bonus_sum = np.sum( upper[ upper>0 ] )
    
    if TYPE == "scand6":
        if bonus_sum>=84:
            bonus_fv = (0,1)
        else:
            bonus_fv = ( 84-bonus_sum ,0) 
    else:
        if bonus_sum>=63:
            bonus_fv = (0,1)
        else:
            bonus_fv = ( 63-bonus_sum ,0)    
    
    y = tuple(x) + tuple(cats) + tuple(cat_values_for_the_given_dices)  + bonus_fv
    
    y_int = []
    for y_i in y:
        y_int.append( int( y_i ) )
    
    return tuple( y_int )


def add_features_scand( x ):
    # ['category_chosen', 'dice_value_0', 'dice_value_1', 'dice_value_2', 'dice_value_3', 'ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy', 'final_score', 'possible_early_finish']
    global col_names
    global numberOfDice
    global categories
    x = list(x)
    print(x)
    # x[ int(x[0])+6 ] = -1
    x[ int(x[col_names.index("category_chosen")])+6 ] = -1
    
    # dices = np.array( x[1:6] )
    dices = np.array( x[col_names.index("dice_value_0"):col_names.index("ones")] )
    print("dices")
    print(dices)
    
    # cats = np.array( x[6:19] )
    cats = np.array( x[col_names.index("ones"):col_names.index("final_score")] )
    cats[ cats>=0 ] = 1
    cats[ cats<0 ] = 0

    # cat_values_for_the_given_dices = np.zeros( (13,) ) 
    cat_values_for_the_given_dices = np.zeros( (len(categories),) ) 
    for i in range(numberOfDice + 1):
        cat_values_for_the_given_dices[i] = np.sum( dices[dices==(i+1)] )

    dice_counts = np.histogram( dices , bins=range(1,8)) [0]
    print(dice_counts)
    
    count_two_ind_onePair = np.where( dice_counts>=2 )[0]
    if len(count_two_ind_onePair)==2:
        cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_two_ind_onePair[0]+1 )
        cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_two_ind_onePair[1]+1 )
    elif len(count_two_ind_onePair)==1:
        cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_two_ind_onePair[0]+1 )

    count_three_ind = np.where( dice_counts>=3 )[0]
    if len(count_three_ind)==1:
        cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_three_ind[0]+1 )
        
        count_two_ind = np.where( dice_counts==2 )[0]
        if len(count_two_ind)==1:
            cat_values_for_the_given_dices[categories.index("fullHouse")] = 3*( count_three_ind[0]+1 ) + 2*( count_two_ind[0]+1 )  

    count_four_ind = np.where( dice_counts>=4 )[0]
    if len(count_four_ind)==1:
        cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_four_ind[0]+1 )
         
    dice_counts_p = np.copy(dice_counts)
    dice_counts_p[ dice_counts_p>0 ] = 1
    if (np.sum( dice_counts_p==np.array( [1,1,1,1,0,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,1,1,1,1,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,0,1,1,1,1] ) )==6)  or (np.sum( dice_counts_p==np.array( [1,0,1,1,1,1] ) )==6) or (np.sum( dice_counts_p==np.array( [1,1,1,1,0,1] ) )==6):
        cat_values_for_the_given_dices[categories.index("smallStraightu")] = 30
        
    if (np.sum( dice_counts==np.array( [0,1,1,1,1,1] ) )==6) or (np.sum( dice_counts==np.array( [1,1,1,1,1,0] ) )==6):
        cat_values_for_the_given_dices[categories.index("largeStraightu")] = 40
        cat_values_for_the_given_dices[categories.index("smallStraightu")] = 30
        
    cat_values_for_the_given_dices[categories.index("chance")] = np.sum( dices )
    
    count_five_ind = np.where( dice_counts==5 )[0]
    if len(count_five_ind)==1:
        cat_values_for_the_given_dices[categories.index("yatzy")] = 50    
    
    print("cat_values_for_the_given_dices")
    print(cat_values_for_the_given_dices)
    print("len(cat_values_for_the_given_dices)")
    print(len(cat_values_for_the_given_dices))
    print("(1-cats)")
    print((1-cats) )
    print("len(cats)")
    print(len(cats))
    cat_values_for_the_given_dices = cat_values_for_the_given_dices*(1-cats) 

    # upper = np.array( x[6:12] )
    upper = np.array( x[col_names.index("ones"):(col_names.index("sixes")+1)] )

    bonus_sum = np.sum( upper[ upper>0 ] )
    
    if bonus_sum>=63:
        bonus_fv = (0,1)
    else:
        bonus_fv = ( 63-bonus_sum ,0)    
    
    y = tuple(x) + tuple(cats) + tuple(cat_values_for_the_given_dices)  + bonus_fv
    
    y_int = []
    for y_i in y:
        y_int.append( int( y_i ) )
    
    return tuple( y_int )

a_ukus = [8, 2, 4, 3, 0, -1, -1, -1, -1, -1, -1, -1, -1, 25, -1, -1, -1, -1, 185]
a_scand = ['2', 3, 4, 3, 3, 3, -1, -1, 12, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 12]
# print(add_features_ukus(a_ukus))
print(add_features_ukus(a_scand))