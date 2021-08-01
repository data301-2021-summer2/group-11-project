#imports
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

sns.set_theme(style="darkgrid",font_scale=1.2)

#Loading and Wrangling individual dataframes
def loadStatCanCPI():
    StatCanCPI = pd.read_csv('./../../data/processed/preprocessed/Stat_Can_CPI_1985_to_Now.csv')
    StatCanCPI = StatCanCPI.set_index('Products and product groups 4')
    return StatCanCPI
def loadStatCanBCHPIData():
    StatCanBCHPI = pd.read_csv('./../../data/processed/preprocessed/Stat_Can_HPI_BC-only_1986_to_2021_May.csv')
    StatCanBCHPI = StatCanBCHPI.drop(index=[1,2],axis=0)
    #Finds just columsn with January in the name
    colWithJan = [col for col in StatCanBCHPI.columns if 'Jan' in col]
    colUpdated = []
    #Maps column name from two-formats in dataset (pre-01 and 01 onward) to standard year-format
    for x in range(len(colWithJan)):
        if colWithJan[x][4:6] != "an":
            if (int(colWithJan[x][4:6])) > 80:
                colUpdated.append("19"+ colWithJan[x][4:6])
            elif int(colWithJan[x][4:6]) == 0:
                colUpdated.append("20" + colWithJan[x][4:6]) 
        else: colUpdated.append("20" + colWithJan[x][0:2])
    #replacing jan colum names with mapped names and dropping rest of columns
    StatCanBCHPI = StatCanBCHPI[colWithJan].set_axis(colUpdated, axis='columns')
    #Chanaging row access to name which better represents data
    StatCanBCHPI = StatCanBCHPI.set_axis(['BC New Housing Price Index'], axis='index')
    return StatCanBCHPI
def loadStatCanCanadaHPIData():
    StatCanCanadaHPI = pd.read_csv('./../../data/processed/preprocessed/Stat_Can_HPI_Canada_1981_to_2021_May.csv')
    return StatCanCanadaHPI
def loadCanadaPrimeInterestRate():
    CanadaPIR = pd.read_csv('./../../data/processed/preprocessed/Canada-Prime-Rate-History.csv')
    #Transposing data to place column names as dates
    CanadaPIR = CanadaPIR.transpose()
    CanadaPIR.columns = CanadaPIR.iloc[0]
    CanadaPIR.drop(index=CanadaPIR.index[0], axis=0, inplace=True)
    #Finds just columsn with January in the name
    colWithJan = [col for col in CanadaPIR.columns if 'Jan' in col]
    colUpdated = []
    #Maps column name from in dataset to standard year-format
    for x in range(len(colWithJan)):
        if (int(colWithJan[x][4:6])) > 80:
             colUpdated.append("19"+ colWithJan[x][4:6])
        elif int(colWithJan[x][4:6]) == 0 or int(colWithJan[x][4:6]) > 0:
            colUpdated.append("20" + colWithJan[x][4:6]) 
    #replacing jan colum names with mapped names and dropping rest of columns
    CanadaPIR = CanadaPIR[colWithJan].set_axis(colUpdated, axis='columns')
    return CanadaPIR
def loadMLSRegionalHPIData():
    MLSHPIData = pd.read_excel('./../../data/processed/preprocessed/MLS HPI - Seasonally Adjusted.xlsx',sheet_name=['AGGREGATE', 'OKANAGAN_VALLEY','GREATER_VANCOUVER'], usecols=['Date', 'One_Storey_Benchmark_SA', 'Two_Storey_Benchmark_SA', 'Townhouse_Benchmark_SA', 'Apartment_Benchmark_SA'])
    #Updating Keys for clarity/redability and alignment to research question
    MLSHPIData['Vancouver'] = MLSHPIData.pop('GREATER_VANCOUVER')
    MLSHPIData['Kelowna'] = MLSHPIData.pop('OKANAGAN_VALLEY')
    MLSHPIData['Canada'] = MLSHPIData.pop('AGGREGATE')
    #Cycling through keys and creating list for changing colums to add regional information, then setting horizontal index to be date in each key-value pair, then replacing column-names with new list
    for key in MLSHPIData.keys():
        colRenameInfo=[key + " " + 'One Storey Home', key + " " + 'Two Storey Home', key + " "  + 'Townhouse', key + " " + 'Apartment']
        MLSHPIData[key].set_index('Date',inplace=True)
        MLSHPIData[key].set_axis(colRenameInfo,axis='columns',inplace=True)
    #Joining all three regions together into one single dataframe
    return MLSHPIData['Vancouver'].join(MLSHPIData['Kelowna'].join(MLSHPIData['Canada']))
def loadWorldBankData():
    WorldBankData = pd.read_csv('./../../data/processed/preprocessed/World Bank Data - Indicators.csv')
    WorldBankData = WorldBankData.set_index('Year')
    #WorldBankData.drop(index=WorldBankData.index[1], axis=0, inplace=True)
    return WorldBankData

def loadAll():
    StatCanCPI = loadStatCanCPI()
    StatCanBCHPI = loadStatCanBCHPIData()
    StatCanCanadaHPI = loadStatCanCanadaHPIData()
    CanadaPIR = loadCanadaPrimeInterestRate()
    WorldBankData = loadWorldBankData()
    dataFrames = [StatCanCPI, StatCanBCHPI, CanadaPIR, WorldBankData]
    return dataFrames
def load_and_process():
    ourData = loadAll()
    #Merging StatCanCPI, StatCanBCHPI, CanadaPIR and WorldBankData all are wrangled into yearly sets.
    masterDF = ourData[0].append(ourData[3].append(ourData[1].append(ourData[2]))).sort_index(axis=1).convert_dtypes(int)
    return masterDF
def limitYears(aDF, backXYears):
    #PresupposesDF is already sorted
    return aDF.iloc[:, aDF.shape[1]-backXYears:aDF.shape[1]-1]
def printStatTableByDecade(aDF, areaOfInterest):
    #This function prints a statistical table
    #Establishing column name list for various decades
    The80s = [col for col in aDF.columns if '198' in col]
    The90s = [col for col in aDF.columns if '199' in col]
    The00s = [col for col in aDF.columns if '200' in col]
    The10s = [col for col in aDF.columns if '201' in col]
    The20s = [col for col in aDF.columns if '202' in col]
    decades = {'80s' : The80s, '90s' :The90s, '00s' :The00s, '10s' :The10s, '20s' :The20s}

    #Choosing area of interest to collect data for
    statData = {}
    for key in decades.keys():
        statData[key] = [aDF[decades[key]].loc[areaOfInterest].mean(),aDF[decades[key]].loc[areaOfInterest].min(),aDF[decades[key]].loc[areaOfInterest].max()]
        
    #Table Print
    TableHeader = "A Summary of Stats " + areaOfInterest + " By Decades in Tabular Form"
    print(TableHeader)
    print("-"*len(TableHeader))
    print("Decade | Mean\t |  Min\t |  Max\t |")
    for key in statData.keys():
        print(key + "    | " + str(statData[key][0])[0:4] + "\t | " + str(statData[key][1])[0:4] + "\t | " + str(statData[key][2])[0:4] + "\t |")
def relPlotOverTime(aDF,areaOfInterest,years,sizex, sizey):
    sns.set(rc={"figure.figsize":(sizex, sizey)})
    sns.relplot(x=limitYears(aDF,years).columns, y=areaOfInterest, data=limitYears(aDF,years).transpose()).set(title="Relational Plot of " + areaOfInterest + " from years " + limitYears(aDF,years).columns.min() + " to " + limitYears(aDF,years).columns.max())
    plt.title = "You suck Andy. Ballllllss."
def scatterPlotOverTime(aDF,areaOfInterest,years,sizex, sizey):
    sns.set(rc={"figure.figsize":(sizex, sizey)})
    sns.scatterplot(x=limitYears(aDF,years).columns, y=areaOfInterest, data=limitYears(aDF,years).transpose()).set(title="Scatter Plot of " + areaOfInterest + " from years " + limitYears(aDF,years).columns.min() + " to " + limitYears(aDF,years).columns.max())
    
