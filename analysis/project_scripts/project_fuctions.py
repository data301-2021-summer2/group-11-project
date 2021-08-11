##############******Shared Project Functions******##############
#**************************************************************#
#              1. Section 1 - Imported Libraries               #
#              2. Section 2 - Data Wrangling and processing    #
#              3. Section 3 - On-demand Wrangling Functions    #
#              4. Section 4 - Statistical Analysis Functions   #
#              5. Section 5 - Graphing functions               #
#**************************************************************#



#                 Section 1 - Imported Libraries               #

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

sns.set_theme(style="darkgrid",font_scale=0.7)



#                 Section 2 - Data Wrangling and processing    #

#Function loads Statistics Canada Consumer Price Index Data via method-chaining. Little processing is required on this dataset.
def loadStatCanCPI():
    #Method Chain
    StatCanCPI = (
        pd.read_csv(
            './../../data/processed/preprocessed/Stat_Can_CPI_1985_to_Now.csv'
        )
        .set_index('Products and product groups 4')
    )
    return StatCanCPI

#Function loads Statistics Canada British Columbia Housing Price Index Data
def loadStatCanBCHPIData():
    #Method Chain
    StatCanBCHPI = (
        pd.read_csv(
            './../../data/processed/preprocessed/Stat_Can_HPI_BC-only_1986_to_2021_May.csv'
        )
        .drop(index=[1,2],axis=0)
    )
    
    #Finds just columns with January in the name
    colWithJan = [col for col in StatCanBCHPI.columns if 'Jan' in col]
    colUpdated = []
    
    #Maps column name from two-formats in dataset (pre-01 and 01 onward) to standard year-format to match other DF.
    for x in range(len(colWithJan)):
        if colWithJan[x][4:6] != "an":
            if (int(colWithJan[x][4:6])) > 80:
                colUpdated.append("19"+ colWithJan[x][4:6])
            elif int(colWithJan[x][4:6]) == 0:
                colUpdated.append("20" + colWithJan[x][4:6]) 
        else: colUpdated.append("20" + colWithJan[x][0:2])
            
    #Method Chain
    #Dropping unused columns, applying column name update made in for loop above and renaming axis.
    StatCanBCHPI = (
        StatCanBCHPI[colWithJan].copy(deep=False)
        .set_axis(colUpdated, axis='columns')
        .set_axis(['BC New Housing Price Index'], axis='index')
    )
    
    return StatCanBCHPI

#Function loads Statistics Canada Canada-wide Housing Price Index Data
def loadStatCanCanadaHPIData():
    StatCanCanadaHPI = (
        pd.read_csv(
        './../../data/processed/preprocessed/Stat_Can_HPI_Canada_1981_to_2021_May.csv'
        )
    )
    return StatCanCanadaHPI

#Function loads Statistics Canada Prime Interest Rate Data
def loadCanadaPrimeInterestRate():
    #Method chaining
    CanadaPIR = (
        pd.read_csv(
            './../../data/processed/preprocessed/Canada-Prime-Rate-History.csv'
        )
        #Transposing data to place column names as dates
        .transpose()
    )
    
    #After much effort, .columns cannot reference CanadaPIR before it is established so had to end-chain to run following command
    CanadaPIR.columns = CanadaPIR.iloc[0]
    CanadaPIR.drop(index = CanadaPIR.index[0], axis=0, inplace=True)
    
    #Finds just columns with str 'Jan' in the name to select January months
    colWithJan = [col for col in CanadaPIR.columns if 'Jan' in col]
    colUpdated = []
    
    #Maps column name from in dataset to standard year-format to match other DF.
    for x in range(len(colWithJan)):
        if (int(colWithJan[x][4:6])) > 80:
             colUpdated.append("19"+ colWithJan[x][4:6])
        elif int(colWithJan[x][4:6]) == 0 or int(colWithJan[x][4:6]) > 0:
            colUpdated.append("20" + colWithJan[x][4:6]) 
              
    #Method Chain
    #Dropping unused columns, applying column name update made in for loop above.
    CanadaPIR = (
        CanadaPIR[colWithJan].copy(deep=False)
        .set_axis(colUpdated, axis='columns')
    )
    
    return CanadaPIR

def loadMLSRegionalHPIData(): 
    #Method Chaining
    MLSHPIData = (
        pd.read_excel(
            './../../data/processed/preprocessed/MLS HPI - Seasonally Adjusted.xlsx',
            sheet_name=['AGGREGATE', 'OKANAGAN_VALLEY','GREATER_VANCOUVER'], 
            usecols=['Date', 'One_Storey_Benchmark_SA', 'Two_Storey_Benchmark_SA', 'Townhouse_Benchmark_SA', 'Apartment_Benchmark_SA']
        )
    )
    
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

#Function loads World Bank Data for Canada regarding Economic Progress Indicators
def loadWorldBankData():
    #Method Chain
    WorldBankData = ( 
        pd.read_csv(
            './../../data/processed/preprocessed/World Bank Data - Indicators.csv'
        )
        .set_index('Year')
    )
        
    return WorldBankData





#                 Section 3 - On-demand Wrangling Functions    #

def loadAll():
    #Loading individual datasets
    StatCanCPI = loadStatCanCPI()
    StatCanBCHPI = loadStatCanBCHPIData()
    StatCanCanadaHPI = loadStatCanCanadaHPIData()
    CanadaPIR = loadCanadaPrimeInterestRate()
    WorldBankData = loadWorldBankData()

    #Creating single returnable from function by generating list
    dataFrames = [StatCanCPI, StatCanBCHPI, CanadaPIR, WorldBankData]
    return dataFrames

#Required function_name according to task document
def load_and_process():
    ourData = loadAll()
    #Merging StatCanCPI, StatCanBCHPI, CanadaPIR and WorldBankData all are pre-wrangled into yearly sets.
    masterDF = ourData[0].append(ourData[3].append(ourData[1].append(ourData[2]))).sort_index(axis=1).convert_dtypes(int)
    return masterDF

#Function takes a dataset and returns a smaller subset of data containing a specific passable number of years from present date.
#This allows less-data for smaller analysis of specific periods of time (easier to see short trends/changes)
def limitYears(aDF, backXYears):
    #PresupposesDF is already sorted
    return aDF.iloc[:, aDF.shape[1]-backXYears:aDF.shape[1]-1]






#                 Section 4 - Statistical Analysis Functions   #

#Stat Printing Table
def printStatTableByDecade(aDF, areaOfInterest):
    #This function prints a statistical table including mean, min, max
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
    print("Decade | Mean\t |  Min   |  Max |")
    for key in statData.keys():
        print(key + "    | " + str(round(statData[key][0],3)) + "\t | " + str(round(statData[key][1],3)) + "\t | " + str(round(statData[key][2],3)) + " |")






#                Section 5 - Graphing functions               #

#Performs a relational plot over a specific period of time from current date.
#Passable arguments are a dataframe to plot from, what column to plot, how many years to plot backwards from current date, size in x and y directions.
def relPlotOverTime(aDF,areaOfInterest,years,sizex, sizey):
    sns.set(rc={"figure.figsize":(sizex, sizey)})
    sns.relplot(x=limitYears(aDF,years).columns, y=areaOfInterest, data=limitYears(aDF,years).transpose()).set(title="Relational Plot of " + areaOfInterest + " from years " + limitYears(aDF,years).columns.min() + " to " + limitYears(aDF,years).columns.max())
    plt.title = "You suck Andy. Ballllllss."

#Performs a scatter plot over a specific period of time from current date.
#Passable arguments are a dataframe to plot from, what column to plot, how many years to plot backwards from current date, size in x and y directions.
def scatterPlotOverTime(aDF,areaOfInterest,years,sizex, sizey):
    sns.set(rc={"figure.figsize":(sizex, sizey)})
    sns.scatterplot(x=limitYears(aDF,years).columns, y=areaOfInterest, data=limitYears(aDF,years).transpose()).set(title="Scatter Plot of " + areaOfInterest + " from years " + limitYears(aDF,years).columns.min() + " to " + limitYears(aDF,years).columns.max())

#Performs a bar plot over a specific period of time from current date.
#Passable arguments are a dataframe to plot from, what column to plot, how many years to plot backwards from current date, size in x and y directions, and color.
def barPlotOverTime(aDF,areaOfInterest,years,sizex,sizey, color):
    sns.barplot(data=limitYears(aDF,years).transpose(), x=limitYears(aDF,years).columns, y=areaOfInterest, color=color).set(title="Scatter Plot of " + areaOfInterest + " from years " + limitYears(aDF,years).columns.min() + " to " + limitYears(aDF,years).columns.max())
    sns.despine()
    sns.set(rc={"figure.figsize":(sizex, sizey)})

#Performs a multiple-series scatter plot over a specific period of time from current date.
#Passable arguments are a dataframe to plot from, what columns to plot, the color of the plot and a plot title.
def multiScatterPlot(aDF, locations, color, title):
    fig, ax = plt.subplots()
    plt.title(title,size=20)

    ax2=[]
    sns.scatterplot(x=aDF.index, y=locations[0], data=aDF, ax=ax, color=color[0], label=locations[0])
    print(range(len(locations)))
    for i in range(len(locations)):
        if not (i==0):
            ax2.append(ax.twinx())
            sns.scatterplot(x=aDF.index, y=locations[i], data=aDF, ax=ax2[i-1], color=color[i], label=locations[i])
        


    lines = [None]*(len(ax2)+1)
    labels = [None]*(len(ax2)+1)
    lines[0], labels[0] = ax.get_legend_handles_labels()
    for i in range(len(ax2)):
        lines[i+1], labels[i+1] = ax2[i].get_legend_handles_labels()

    line_pass = lines[0]
    labels_pass = labels[0]


    for i in range(len(labels)):
        if not (i==0):
            line_pass = line_pass +  lines[i]
            labels_pass = labels_pass + labels[i]
    
    ax2[len(ax2) - 1].legend(line_pass, labels_pass, loc='upper left',fontsize='20')

    ax.axes.get_yaxis().set_visible(False)
    for i in range(len(ax2)):
        ax2[i].axes.get_yaxis().set_visible(False)

    plt.show()

#Performs a side by side scatter plot over a specific period of time from current date to compare passed columns.
#Passable arguments are a dataframe to plot from, what columns to plot, of the overall plot title.
def sideBySideScatterPlot(aDF,columns, title):
    fig, axs = plt.subplots(ncols=len(columns))
    fig.set_figwidth(30)
    fig.set_figheight(15)
    plt.title(title,size=20)
    for location in range(len(columns)):
        sns.scatterplot(y=aDF[columns[location-1]],x=aDF.index,data=aDF,ax=axs[location-1])

#Normalizes columns from values of 0 to 1 and then produces heatmap for comparison.
#Passable arguments are a dataframe to plot from, what columns to plot, and years to plot over.
def normalizedHeatMap(aDF, columns, years, title):
    a=[]
    #Normalizing values between 0 to 1 for heatmap comparison by dividing individual entry by max entry in column.
    for aCol in columns:
        a.append(limitYears(aDF,years).loc[aCol].transpose() / limitYears(aDF,years).loc[aCol].transpose().max())

    f, ax = plt.subplots()
    plt.title(title,size=20)
    ax = sns.heatmap(pd.DataFrame(a, dtype="float"))

#Normalizes columns from values of 0 to 1 and then produces heatmap for comparison.
#Passable arguments are a dataframe to plot from, what columns to plot, of the overall plot title.
def normalizedHeatMap2(aDF, columns, title):
    a=[]
    #Normalizing values between 0 to 1 for heatmap comparison by dividing individual entry by max entry in column.
    for aCol in columns:
        a.append(aDF[aCol] / aDF[aCol].max())

    f, ax = plt.subplots()
    plt.title(title,size=20)
    ax = sns.heatmap(pd.DataFrame(a, dtype="float"),xticklabels=False)