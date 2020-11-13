#%pyspark
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col
import json
from datetime import datetime
import pandas as pd
import numpy as np
import boto3


TYPE = "scand6"
s3 = boto3.resource('s3')

if TYPE == "ukus":
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy']
    bonus_value = 35
    numberOfDice = 5
    content_object = s3.Object('yatzy-simdata', 'levels/dict_yatzy_ukus_A')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    levels_dictionary = json.loads(file_content)
elif TYPE == "scand":
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'onePair', 'twoPairs', 'threeOfAKind', 'fourOfAKind', 'smallStraight', 'largeStraight', 'fullHouse', 'chance', 'yatzy']
    bonus_value = 50   
    numberOfDice = 5
    content_object = s3.Object('yatzy-simdata', 'levels/dict_yatzy_scand_A')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    levels_dictionary = json.loads(file_content)
else:
    numberOfDice = 6
    categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'onePair', 'twoPairs', 'threePairs', 'threeOfAKind', 'fourOfAKind', 'fiveOfAKind', 'smallStraight', 'largeStraight', 'fullStraight', 'fullHouse', 'threePlusThree', 'fourPlusTwo', 'chance', 'yatzym']
    bonus_value = 100
    content_object = s3.Object('yatzy-simdata', 'levels/dict_yatzy_scand6_A')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    levels_dictionary = json.loads(file_content)


col_names = ["category_chosen"]
for k in range(numberOfDice):
    col_names.append("dice_value_" + str(k))
for k in categories:
    col_names.append(str(k))
col_names.append("final_score")
col_names.append("possible_early_finish")


def replace_dice_values(dataframe, number_of_dices):
    copy_dataframe = dataframe.sort_values(by=['revision'])
    # copy_dataframe = dataframe
    # copy_dataframe = copy_dataframe.reset_index(drop=True)
    copy_dataframe = pd.DataFrame(copy_dataframe.values, columns = copy_dataframe.columns)
    # copy_dataframe= copy_dataframe.set_index(np.arange(len(copy_dataframe.index)))
    lastaction_indices = copy_dataframe.index[copy_dataframe['lastaction'] == "storeAndFinish"].tolist()
    for i in lastaction_indices:
        try:
            for j in range(0, number_of_dices):
                if copy_dataframe['revision'][i] == copy_dataframe['revision'][i - 1] + 1:
                    copy_dataframe['dice_lock_' + str(j)][i] = copy_dataframe['dice_lock_' + str(j)][i - 1]
                    copy_dataframe['dice_value_' + str(j)][i] = copy_dataframe['dice_value_' + str(j)][i - 1]
        except:
            continue
    return copy_dataframe
    
def select_needed_columns(numberOfDice, categories, header, dataF):
    col_nms = []
    if header and not dataF.empty:
        col_nms = ["category_chosen"]
        for k in range(numberOfDice):
            col_nms.append("dice_value_" + str(k))
        for k in categories:
            col_nms.append(k)
        col_nms.append("final_score")
        df1 = dataF[col_nms]
        return df1

def game_to_csv_version2(data):
    global numberOfDice
    global bonus_value
    global categories
    global TYPE
    
    # Get final_score and possible_early_finish game
    revision_list = []
    for item in data:
        temporary_move = item.get('game')
        revision_list.append(temporary_move.get('revision', 0))
    revision_max_index = revision_list.index(max(revision_list))
    # The greatest revision number also gives us the final score of each gamer
    final_score_temp_var = data[revision_max_index].get("game")
    number_of_players = final_score_temp_var.get("numberOfPlayers")
    players_list = [{}] * number_of_players
    final_score_temp_var = final_score_temp_var.get('players')
    final_score = []
    # A variable that states if maybe the game was exited/surrendered earlier (by default true)
    possible_early_finish = True
    # For each player make a dictionary of its ID, Final_score and possible_early_finish
    for k in final_score_temp_var:
        player_id = k.get("id")
        final_score_temp_var2 = k.get("gameplayer", 0)
        final_score_temp_var3 = final_score_temp_var2.get("currentScore", 0)
        list_of_category_values = list(final_score_temp_var3.values())
        # If the number of played categories is equal to the number of categories of the game played, then the game was not terminated earlier
        if len(list_of_category_values) == (len(categories) - 1) and possible_early_finish != False:
            possible_early_finish = False
        final_score_value = sum(list_of_category_values)
        if final_score_temp_var2.get('haveReceivedBonus') == True:
            final_score_value = final_score_value + bonus_value
        final_score.append({"player_id": player_id, "final_score" : final_score_value})

    csv_list = []
    for item in data:
        game = item.get("game")
        players = game.get("players")
        for k in range(0, len(players)):
            players_list[k]["category_chosen"] = "nada"
            players_list[k].update(game_id = item.get("_id"))
            players_list[k].update(gamevariable = item.get("gamevariable"))
            players_list[k].update(numberOfPlayers = game.get("numberOfPlayers"))
            players_list[k].update(gameStatus = game.get("gameStatus"))
            # players_list[k].update(activePlayer = game.get("activePlayer"))
            players_list[k].update(revision = game.get("revision"))
            player_id = players[k].get("id")
            players_list[k].update(player_id = player_id)
            players_list[k].update(player_name = players[k].get("name"))
            gameplayer = players[k].get("gameplayer")
            # players_list[k].update(roll = gameplayer.get("roll"))
            players_list[k].update(haveReceivedBonus = gameplayer.get("haveReceivedBonus"))
            if gameplayer.get('haveReceivedBonus') == True:
                bonus = bonus_value
            else:
                bonus = -1
            players_list[k].update(lastaction = gameplayer.get("lastaction"))
            players_list[k].update(gameStatus = gameplayer.get("gameStatus"))
            dices = gameplayer.get("dice")
            numberOfDice = dices.get("numberOfDice")
            players_list[k].update(numberOfDice = numberOfDice)
            dices = dices.get("dice")
            for i in range(0, len(dices)):
                variable1 = "dice_id_" + str(i)
                d1 = {variable1 : dices[i].get("id")}
                players_list[k].update(d1)
                variable1 = "dice_value_" + str(i)
                d1 = {variable1 : dices[i].get("value")}
                players_list[k].update(d1)
                variable1 = "dice_lock_" + str(i)
                d1 = {variable1 : dices[i].get("diceLock")}
                players_list[k].update(d1)
            score_tv = gameplayer.get('currentScore')
            # Values of the categories we have so far
            categories_values = []
            for m in range(0, len(categories)):
                categories_values.append(score_tv.get(categories[m], -1))
                value = int(score_tv.get(categories[m], -1))
                d1 = {str(categories[m]): value}
                players_list[k].update(d1)
            current_score = sum(num for num in categories_values if num >= 0)
            if bonus != -1:
                current_score = current_score + bonus
            players_list[k].update(current_score = current_score)
            for n in final_score:
                if n.get("player_id") == player_id:
                    players_list[k].update(final_score = n.get("final_score"))
            category_chosen = gameplayer.get("lastvariable", "NaN")
            if category_chosen not in categories:
                players_list[k].update(category_chosen = "NaN")
            else:
                ind = categories.index(category_chosen)
                #players_list[k].update(category_chosen = ind )
                players_list[k]["category_chosen"] = ind 
            players_list[k].update(possible_early_finish = possible_early_finish)
            csv_list.append(tuple(players_list[k].values()))
    header = list(players_list[0].keys())
    df = pd.DataFrame(csv_list, columns = header)
    df1 = df.drop_duplicates(subset=df.columns.difference(['revision']))
    modified_dataframe = replace_dice_values(df1, numberOfDice)
    #modified_dataframe = df
    deleted_columns_dataframe = modified_dataframe
    deleted_columns_dataframe = select_needed_columns(numberOfDice, categories, header, modified_dataframe)
    new_list = [tuple(x) for x in deleted_columns_dataframe.values]
    return header, new_list

def parse_and_select_type(l):
    global TYPE
    global levels_dictionary
    
    easy = levels_dictionary["easy_" + str(TYPE)]
    medium = levels_dictionary["medium_" + str(TYPE)]
    hard = levels_dictionary["hard_" + str(TYPE)]
    easy_games = []
    medium_games = []
    hard_games = [] 
    
    l = l.replace( "}{", "},{" )
    S = "[" + str(l) + "]"
    jsons_list = json.loads(S)
    
    for x in jsons_list:  
        if x['gamevariable']==TYPE:
            players = x["players"] 
            players_names = [ sub['name'] for sub in players ] 
            if all(elem in easy for elem in players_names):
                easy_games.append(x)
            elif all(elem in medium for elem in players_names):
                medium_games.append(x)
            elif all(elem in hard for elem in players_names):
                hard_games.append(x)
            if players_names[0] in easy:
                y = x
                y["game"]["players"] = list(x["game"]["players"][0])
                easy_games.append(y)
            elif players_names[0] in medium:
                y = x
                y["game"]["players"] = list(x["game"]["players"][0])
                medium_games.append(y)
            elif players_names[0] in hard:
                y = x
                y["game"]["players"] = list(x["game"]["players"][0])
                hard_games.append(y)
            if players_names[1] in easy:
                y = x
                y["game"]["players"] = list(x["game"]["players"][1])
                easy_games.append(y)
            elif players_names[1] in medium:
                y = x
                y["game"]["players"] = list(x["game"]["players"][1])
                medium_games.append(y)
            elif players_names[1] in hard:
                y = x
                y["game"]["players"] = list(x["game"]["players"][1])
                hard_games.append(y)
    s3 = boto3.client('s3')
    s3.put_object(
         Body=str(json.dumps(easy_games.collectAsMap())),
         Bucket='yatzy-simdata',
         Key='testinggg/easy_games'
    )
    s3.put_object(
         Body=str(json.dumps(medium_games.collectAsMap())),
         Bucket='yatzy-simdata',
         Key='testinggg/medium_games'
    )
    s3.put_object(
         Body=str(json.dumps(hard_games.collectAsMap())),
         Bucket='yatzy-simdata',
         Key='testinggg/hard_games'
    )
    return [easy_games, medium_games, hard_games]

# def parse_and_select_type(l):
#     global TYPE
#     selected = []
    
#     l = l.replace( "}{", "},{" )
#     S = "[" + str(l) + "]"
#     jsons_list = json.loads(S)
    
#     for x in jsons_list:  
#         if x['gamevariable']==TYPE:
#             selected.append( x )
#     return selected

def add_features( x ):
    x = list(x)
    x[ int(x[0])+6 ] = -1
    
    dices = np.array( x[1:6] )
    
    cats = np.array( x[6:19] )
    cats[ cats>=0 ] = 1
    cats[ cats<0 ] = 0
    
    #categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'bonus', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy']
    #categories = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy']

    cat_values_for_the_given_dices = np.zeros( (13,) ) 
    for i in range(6):
        cat_values_for_the_given_dices[i] = np.sum( dices[dices==(i+1)] )
        
    dice_counts = np.histogram( dices , bins=range(1,8)) [0]
    
    count_three_ind = np.where( dice_counts>=3 )[0]
    if len(count_three_ind)==1:
        cat_values_for_the_given_dices[6] = 3*( count_three_ind[0]+1 )
        
        count_two_ind = np.where( dice_counts==2 )[0]
        if len(count_two_ind)==1:
            cat_values_for_the_given_dices[8] = 3*( count_three_ind[0]+1 ) + 2*( count_two_ind[0]+1 )  

    count_four_ind = np.where( dice_counts>=4 )[0]
    if len(count_four_ind)==1:
        cat_values_for_the_given_dices[7] = 4*( count_four_ind[0]+1 )
        
    dice_counts_p = np.copy(dice_counts)
    dice_counts_p[ dice_counts_p>0 ] = 1
    if (np.sum( dice_counts_p==np.array( [1,1,1,1,0,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,1,1,1,1,0] ) )==6) or (np.sum( dice_counts_p==np.array( [0,0,1,1,1,1] ) )==6)  or (np.sum( dice_counts_p==np.array( [1,0,1,1,1,1] ) )==6) or (np.sum( dice_counts_p==np.array( [1,1,1,1,0,1] ) )==6):
        cat_values_for_the_given_dices[9] = 30
        
    if (np.sum( dice_counts==np.array( [0,1,1,1,1,1] ) )==6) or (np.sum( dice_counts==np.array( [1,1,1,1,1,0] ) )==6):
        cat_values_for_the_given_dices[10] = 40
        cat_values_for_the_given_dices[9] = 30
        
    cat_values_for_the_given_dices[11] = np.sum( dices )
    
    count_five_ind = np.where( dice_counts==5 )[0]
    if len(count_five_ind)==1:
        cat_values_for_the_given_dices[12] = 50    
    
    cat_values_for_the_given_dices = cat_values_for_the_given_dices*(1-cats) 
    
    upper = np.array( x[6:12] )
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


def add_features_modified( x ):
    # ['category_chosen', 'dice_value_0', 'dice_value_1', 'dice_value_2', 'dice_value_3', 'ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy', 'final_score', 'possible_early_finish']
    global col_names
    global numberOfDice
    global categories
    global TYPE
    x = list(x)

    # x[ int(x[0])+6 ] = -1
    # x[ int(x[col_names.index("category_chosen")])+6 ] = -1
    
    # dices = np.array( x[1:6] )
    dices = np.array( x[col_names.index("dice_value_0"):col_names.index("ones")] )
    
    # cats = np.array( x[6:19] )
    # print("col_names.index(ones)")
    # print(col_names.index("ones"))
    # print("col_names.index(final_score)")
    # print(col_names.index("final_score"))
    cats = np.array( x[col_names.index("ones"):col_names.index("final_score")] )
    cats[ cats>=0 ] = 1
    cats[ cats<0 ] = 0

    # cat_values_for_the_given_dices = np.zeros( (13,) ) 
    # print("len(categories)")
    # print(len(categories))
    cat_values_for_the_given_dices = np.zeros( (len(categories),) ) 
    for i in range(numberOfDice + 1):
        cat_values_for_the_given_dices[i] = np.sum( dices[dices==(i+1)] )

    dice_counts = np.histogram( dices , bins=range(1,8)) [0]

    count_two_ind = np.where( dice_counts==2 )[0]
    if len(count_two_ind)==3:
        cat_values_for_the_given_dices[categories.index("threePairs")] = 2*( count_two_ind[-1]+1 ) + 2*( count_two_ind[-2]+1 ) + 2*( count_two_ind[-3]+1 )
        if 2*( count_two_ind[-1]+1 ) + 2*( count_two_ind[-2]+1 ) > cat_values_for_the_given_dices[categories.index("twoPairs")]:
            cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_two_ind[-1]+1 ) + 2*( count_two_ind[-2]+1 )
        if 2*( count_two_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_two_ind[-1]+1 )
    
    if len(count_two_ind)==2:
        if 2*( count_two_ind[-1]+1 ) + 2*( count_two_ind[-2]+1 ) > cat_values_for_the_given_dices[categories.index("twoPairs")]:
            cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_two_ind[-1]+1 ) + 2*( count_two_ind[-2]+1 )
        if 2*( count_two_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_two_ind[-1]+1 )

    if len(count_two_ind)==1:
        if 2*( count_two_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_two_ind[-1]+1 )
    
    count_three_ind = np.where( dice_counts==3 )[0]
    if len(count_three_ind)>1:
        if 2*( count_three_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_three_ind[-1]+1 )
        if 2*( count_three_ind[-1]+1 ) + 2*( count_three_ind[-2]+1 ) > cat_values_for_the_given_dices[categories.index("twoPairs")]:
            cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_three_ind[-1]+1 ) + 2*( count_three_ind[-2]+1 )
        cat_values_for_the_given_dices[categories.index("threePlusThree")] = 3*( count_three_ind[-1]+1 ) + 3*( count_three_ind[-2]+1 )
        cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_three_ind[-1]+1 )
    if len(count_three_ind)==1:
        count_rest_two = np.where( dice_counts==2 )[0]
        cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_three_ind[-1]+1 )
        if "onePair" in categories:
            if 2*( count_three_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
                cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_three_ind[-1]+1 )
        if len(count_rest_two) > 0:
            if "twoPairs" in categories:
                if (2*( count_three_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 )) > cat_values_for_the_given_dices[categories.index("twoPairs")] :
                    cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_three_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 )
            cat_values_for_the_given_dices[categories.index("fullHouse")] = 3*( count_three_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 ) 
    
    count_four_ind = np.where( dice_counts==4 )[0]
    if len(count_four_ind)==1:
        count_rest_two = np.where( dice_counts==2 )[0]
        if len(count_rest_two) > 0:
            if "twoPairs" in categories:
                if (2*( count_four_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 )) > cat_values_for_the_given_dices[categories.index("twoPairs")]:
                    cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_four_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 )
            if "fourPlusTwo" in categories:
                cat_values_for_the_given_dices[categories.index("fourPlusTwo")] = 4*( count_four_ind[-1]+1 ) + 2*( count_rest_two[-1]+1 )  
        if "onePair" in categories:
            if (2*( count_four_ind[-1]+1 )) > cat_values_for_the_given_dices[categories.index("onePair")]:
                cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_four_ind[-1]+1 )
        if 3*( count_four_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("threeOfAKind")]:
            cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_four_ind[-1]+1 )
        if 4*( count_four_ind[0]+1 ) > cat_values_for_the_given_dices[categories.index("fourOfAKind")]:
            cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_four_ind[0]+1 )

    count_five_ind = np.where( dice_counts==5 )[0]
    if len(count_five_ind)==1:
        if "yatzy" in categories:
            cat_values_for_the_given_dices[categories.index("yatzy")] = 50
        if "onePair" in categories:
            if 2*( count_five_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
                cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_five_ind[-1]+1 )
        if "fiveOfAKind" in categories:
            cat_values_for_the_given_dices[categories.index("fiveOfAKind")] = 5*( count_five_ind[0]+1 )
        if 3*( count_five_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("threeOfAKind")]:
            cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_five_ind[-1]+1 )
        if 4*( count_five_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("fourOfAKind")]:
            cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_five_ind[-1]+1 )

    count_six_ind = np.where( dice_counts==6 )[0]
    if len(count_six_ind)==1:
        cat_values_for_the_given_dices[categories.index("yatzym")] = 100
        if 2*( count_six_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("onePair")]:
            cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_six_ind[-1]+1 )
        if 3*( count_six_ind[-1]+1 ) > cat_values_for_the_given_dices[categories.index("threeOfAKind")]:
            cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_six_ind[-1]+1 )
        if 4*( count_six_ind[0]+1 ) > cat_values_for_the_given_dices[categories.index("fourOfAKind")]:
            cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_six_ind[0]+1 )
        if "fiveOfAKind" in categories:
            if 5*( count_six_ind[0]+1 ) > cat_values_for_the_given_dices[categories.index("fiveOfAKind")]:
                cat_values_for_the_given_dices[categories.index("fiveOfAKind")] = 5*( count_six_ind[0]+1 )

    # # One pair, Two pairs and Three pairs
    # if TYPE == "scand" or TYPE == "scand6":
    #     count_pairs = np.where( dice_counts>=2 )[0]
    #     if len(count_pairs)>=2:
    #         cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_pairs[-1]+1 )
    #         cat_values_for_the_given_dices[categories.index("twoPairs")] = 2*( count_pairs[-1]+1 ) + 2*( count_pairs[-2]+1 )
    #     if len(count_pairs)>=1:
    #         cat_values_for_the_given_dices[categories.index("onePair")] = 2*( count_pairs[-1]+1 )
    #     if TYPE == "scand6":
    #         if len(count_pairs)==3:
    #             cat_values_for_the_given_dices[categories.index("threePairs")] = 2*( count_pairs[-1]+1 ) + 2*( count_pairs[-2]+1 ) + 2*( count_pairs[-3]+1 )
    #         count_three_plus_three = np.where( dice_counts>=3 )[0]
    #         if len(count_three_plus_three) == 2:
    #             cat_values_for_the_given_dices[categories.index("threePlusThree")] = 3*( count_three_plus_three[-1]+1 ) + 3*( count_three_plus_three[-2]+1 )
    #         count_fiveOfAKind = np.where( dice_counts==5 )[0]
    #         if len(count_fiveOfAKind) == 1:
    #             cat_values_for_the_given_dices[categories.index("fiveOfAKind")] = 5*( count_fiveOfAKind[-1]+1 )


    # count_three_ind = np.where( dice_counts>=3 )[0]
    # if len(count_three_ind)==1:
    #     cat_values_for_the_given_dices[categories.index("threeOfAKind")] = 3*( count_three_ind[0]+1 )
        
    #     count_two_ind = np.where( dice_counts==2 )[0]
    #     if len(count_two_ind)==1:
    #         cat_values_for_the_given_dices[categories.index("fullHouse")] = 3*( count_three_ind[0]+1 ) + 2*( count_two_ind[0]+1 )  

    # count_four_ind = np.where( dice_counts>=4 )[0]
    # if len(count_four_ind)==1:
    #     cat_values_for_the_given_dices[categories.index("fourOfAKind")] = 4*( count_four_ind[0]+1 )
    #     if TYPE == "scand6":
    #         count_rest_two = np.where( dice_counts==2 )[0]
    #         if len(count_rest_two) > 0:
    #             cat_values_for_the_given_dices[categories.index("fourPlusTwo")] = 4*( count_four_ind[0]+1 ) + 2*( count_rest_two[0]+1 ) 
                


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
    if TYPE == "scand6" or TYPE == "scand":
        if len(count_different) >= 5 and dice_counts[categories.index("twos")] > 0 and dice_counts[categories.index("threes")] > 0 and dice_counts[categories.index("fours")] > 0 and dice_counts[categories.index("fives")] > 0:
            cat_values_for_the_given_dices[categories.index("largeStraight")] = 20
            cat_values_for_the_given_dices[categories.index("smallStraight")] = 15
        if len(count_different) == 6:
            cat_values_for_the_given_dices[categories.index("fullStraight")] = 21


        
    cat_values_for_the_given_dices[categories.index("chance")] = np.sum( dices )
    
    # if TYPE == "ukus" or TYPE == "scand":
    #     count_five_ind = np.where( dice_counts==5 )[0]
    #     if len(count_five_ind)==1:
    #         cat_values_for_the_given_dices[categories.index("yatzy")] = 50    
    # if TYPE == "scand6":
    #     count_six_ind = np.where( dice_counts==6 )[0]
    #     if len(count_six_ind)==1:
    #         cat_values_for_the_given_dices[categories.index("yatzym")] = 100
    
    # print("cats")
    # print(cats)
    # print(len(cats))
    # print("cat_values_for_the_given_dices")
    # print(cat_values_for_the_given_dices)
    # print(len(cat_values_for_the_given_dices))
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


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

days = [[]]*7
days_1 = list(range(10,32))
#days_1.remove(12)
days[1] = days_1
days[2] = list(range(1,30))
days[3] = list(range(1,32))
days[4] = list(range(1,31))
days[5] = list(range(1,32))
days[6] = list(range(1,27))

for i_month in range(1,2):

    for i_day in range(len(days[i_month])):
        files = "s3://com.miraclemill.yatzy-ml/2020/"+ str(i_month).zfill(2) +"/" + str(days[i_month][i_day]).zfill(2) + "/*/*"
        
        # lines = sc.textFile("s3://com.miraclemill.yatzy-ml/testing_file/*")
        lines = sc.textFile(files)
        
        games_rdd = lines.flatMap( lambda j:  parse_and_select_type( j) )
        gamees = games_rdd.map( lambda elem: list(elem))
        games = list(gamees)
        
        for k in range(0,len(games)):
            jsons_grouped_per_game = games[k].groupBy( lambda a_json: a_json["_id"] )
            jsons_grouped_per_game = jsons_grouped_per_game.map(lambda x : (x[0], list(x[1])))
            
            # header = lambda j:  game_to_csv_version2(j[1], TYPE)[0]
            csv_data = jsons_grouped_per_game.flatMap( lambda j:  game_to_csv_version2(j[1])[1] )
            
            df_csv_data = csv_data.toDF()
            
            date_time_stamp = str(datetime.now())
            date_time_stamp = date_time_stamp.replace(':', '-').replace('.', '-').replace(' ', '--')
            
            # col_inds = [0, 12, 15, 18, 21, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]
    
            # selected_cols = []
            # for x in col_inds:
            #     selected_cols.append( df_csv_data.columns[ x ] )    
            # df_csv_data = df_csv_data.select( *selected_cols )
            
            # df_csv_data = df_csv_data.filter( (col( df_csv_data.columns[0] ) != "NaN") & (col( df_csv_data.columns[-2] ) > 230) )
            df_csv_data = df_csv_data.filter( (col( df_csv_data.columns[col_names.index("category_chosen")] ) != "NaN") )
            
            # df_csv_data = df_csv_data.filter( col( df_csv_data.columns[col_names.index("dice_value_0")] ) != 0 )
            if df_csv_data.rdd.isEmpty():
                continue
            # df_csv_data=df_csv_data.drop( df_csv_data.columns[col_names.index("possible_early_finish")] )
            df_csv_data = df_csv_data.rdd.map( add_features_modified ).toDF()
            
            df_csv_data=df_csv_data.drop( df_csv_data.columns[col_names.index("final_score")] ) # drop the total score
            
            # df_csv_data.coalesce(1).write.csv("s3://aws-glue-test-ml-yatzy/SCANDINAVIAN_FIVE_DICE_GAME/test_code/" + str(date_time_stamp) + "/")
            if k == 0:
                df_csv_data.coalesce(1).write.csv("s3://aws-glue-test-ml-yatzy/SIX_DICE_GAME/level_easy/" + str(i_month)  + "/" + str( days[i_month][i_day] ))
            elif k == 1:
                df_csv_data.coalesce(1).write.csv("s3://aws-glue-test-ml-yatzy/SIX_DICE_GAME/level_medium/" + str(i_month)  + "/" + str( days[i_month][i_day] ))
            elif k == 2:
                df_csv_data.coalesce(1).write.csv("s3://aws-glue-test-ml-yatzy/SIX_DICE_GAME/level_hard/" + str(i_month)  + "/" + str( days[i_month][i_day] ))
            
