#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import time
import pandas as pd
import pickle
import os
import gc

action_1_path = "./data/JData_Action_201602.csv"
action_2_path = "./data/JData_Action_201603.csv"
action_3_path = "./data/JData_Action_201604.csv"
comment_path = "./data/JData_Comment.csv"
product_path = "./data/JData_Product.csv"
user_path = "./data/JData_User.csv"


def get_actions_path(path,start_date, end_date): 
    print "%s %s %s %s"%(path,start_date,end_date,time.strftime('%H:%M:%S',time.localtime(time.time())))
    dump_path = './cache/all_action_%s_%s_%s.pkl' % (path[20:26],start_date, end_date) 
    action_chunks = pd.read_csv(path,chunksize=1000000,dtype={'user_id':int,'sku_id':int,'type':int}) 
    actions=pd.read_csv(path,nrows=1,dtype={'user_id':int,'sku_id':int,'type':int}) 
    flag=0
    for chunk in action_chunks: 
        if(chunk.iloc[-1]['time']<start_date): 
            continue 
        if(chunk.iloc[0]['time']>end_date): 
            return actions 
        actions=actions.append(chunk[(chunk['time']>=start_date)&(chunk['time']<=end_date)]) 
        if(flag==0):
            flag=1
            actions=actions.iloc[1:]
                          
    pickle.dump(actions, open(dump_path, 'w'))   
    return actions


def get_actions(start_date, end_date): 
    """ 

    :param start_date: 
    :param end_date: 
    :return: actions: pd.Dataframe 
    """ 
    #dump_path = './cache/all_action_%s_%s.pkl' % (start_date, end_date) 
    csv_path = './cache/all_action_%s_%s.csv' % (start_date, end_date) 
    if os.path.exists(csv_path): 
        actions = pd.read_csv(csv_path)
    else: 
        if(start_date<'2016-03-01'): 
            if(end_date<'2016-03-01'): 
                actions = get_actions_path(action_1_path,start_date, end_date) 
            elif(end_date<'2016-04-01'): 
                action_1 = get_actions_path(action_1_path,start_date, end_date) 
                action_2 = get_actions_path(action_2_path,start_date, end_date) 
                actions = pd.concat([action_1, action_2]) # type: pd.DataFrame 
            else: 
                action_1 = get_actions_path(action_1_path,start_date, end_date) 
                action_2 = get_actions_path(action_2_path,start_date, end_date) 
                action_3 = get_actions_path(action_3_path,start_date, end_date) 
                actions = pd.concat([action_1, action_2,action_3]) # type: pd.DataFrame 
        elif(start_date<'2016-04-01'): 
            if(end_date<'2016-04-01'): 
                actions = get_actions_path(action_2_path,start_date, end_date) 
            else: 
                action_2 = get_actions_path(action_2_path,start_date, end_date) 
                action_3 = get_actions_path(action_3_path,start_date, end_date) 
                actions = pd.concat([action_2, action_3]) # type: pd.DataFrame 
        else: 
            actions = get_actions_path(action_3_path,start_date, end_date) 

        #pickle.dump(actions, open(dump_path, 'w')) 
        actions.to_csv(csv_path,index=False,chunksize=1000000)
    return actions 


def get_actions_user(path,user):
    csv_path = './cache/all_action_user_%s.csv' % path[-6:-4] 
    if os.path.exists(csv_path): 
        actions = pd.read_csv(csv_path)
    else: 
        action1 = pd.read_csv(path,dtype={'user_id':int,'sku_id':int,'type':int}) #11485424
        action1=pd.merge(action1,user,on='user_id',how='inner')#6908805
        #action1_buy=action1[action1['type']==4]#5694
        #tmp=action1_buy.groupby(['user_id','sku_id'],as_index=False).count()
        #tmp[tmp['type']>1] #大部分只购买过一次，154个用户商品购买过两次及以上
        #action1_buy.loc[252]  Series 252是索引
        action_skugp=action1.groupby(['sku_id'],as_index=False)#671018
        
        flag=0
        #一次性把671018个元素装进a，4G内存无法处理
        #56572个user，15444个sku，按sku进行分组好了，一个sku处理完就写到csv中
        for sku,skugroup in action_skugp:
            a=[]
            flag=flag+1
            print flag
            action1gp=skugroup.groupby(['user_id','sku_id'],as_index=False)
            for name,group in action1gp:
                tmp1=group[group['type']==4]
                if(len(tmp1)==0):
                    df = pd.get_dummies(group['type'], prefix='action')
                    group = pd.concat([group, df], axis=1) 
                    groupgp = group.groupby(['user_id', 'sku_id'], as_index=False).sum()
                    #groupgp['type']=0
                    groupgp['start_time']=group.iloc[0]['time']
                    groupgp['end_time']=group.iloc[-1]['time']
                    a.append(pd.DataFrame(groupgp,columns=['user_id','sku_id','action_1','action_2','action_3','action_4','action_5','action_6','start_time','end_time']))
                else:
                    tmp2=group.loc[:tmp1.index[0]]
                    df = pd.get_dummies(tmp2['type'], prefix='action')
                    tmp2=pd.concat([tmp2, df], axis=1) 
                    tmp2 = tmp2.groupby(['user_id', 'sku_id'], as_index=False).sum()
                    tmp2['start_time']=group.iloc[0]['time']
                    tmp2['end_time']=group.loc[tmp1.index[0]]['time']
                    a.append(pd.DataFrame(tmp2,columns=['user_id','sku_id','action_1','action_2','action_3','action_4','action_5','action_6','start_time','end_time']))
                    for i in range(1,len(tmp1)):
                        tmp2=group.loc[tmp1.index[i-1]+1:tmp1.index[i]]
                        df = pd.get_dummies(tmp2['type'], prefix='action')
                        tmp2=pd.concat([tmp2, df], axis=1) 
                        tmp2 = tmp2.groupby(['user_id', 'sku_id'], as_index=False).sum()
                        tmp2['start_time']=group.loc[tmp1.index[i-1]]['time']
                        tmp2['end_time']=group.loc[tmp1.index[i]]['time']
                        a.append(pd.DataFrame(tmp2,columns=['user_id','sku_id','action_1','action_2','action_3','action_4','action_5','action_6','start_time','end_time']))
                    tmp2=group.loc[tmp1.index[-1]:]
                    if(len(tmp2)>0):
                        df = pd.get_dummies(tmp2['type'], prefix='action')
                        tmp2=pd.concat([tmp2, df], axis=1) 
                        tmp2 = tmp2.groupby(['user_id', 'sku_id'], as_index=False).sum()
                        tmp2['start_time']=group.loc[tmp1.index[-1]]['time']
                        tmp2['end_time']=group.iloc[-1]['time']
                        a.append(pd.DataFrame(tmp2,columns=['user_id','sku_id','action_1','action_2','action_3','action_4','action_5','action_6','start_time','end_time']))
                        
            result=pd.concat(a)
            if(flag==1):
                result.to_csv(csv_path,mode='a',header=True,index=False)
            else:
                result.to_csv(csv_path,mode='a',header=False,index=False)

    return 1


actions=get_actions('2016-04-01','2016-05-01')
user=actions[['user_id']]
user=user.drop_duplicates()
del actions
gc.collect()

get_actions_user(action_1_path,user)
