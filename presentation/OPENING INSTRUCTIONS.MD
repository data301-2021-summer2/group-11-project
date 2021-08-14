# IMPORTANT

# IMPORTANT

# IMPORTANT

## As Tableau is a ...piece of quality software, this file generally will not open a even when the database as been set to USE RELATIVE PATHS it uses ABSOLUTE PATHS.

## Consequently, it will try to open c:\users\wburc\documents\github\data301\group-11-project\data\processed\final.df. THIS FILE DOES NOT EXIST on your cloned respository.

## To fix this, do the following steps:

1. Data - > Add new Data Source, browse to the cloned respository directory > then /data/processed/final.DF
1. Select a name such as FinalDF Good or Final DF two.
1. Next click DATA INTERPRETER CHECKBOX on the left hand side.
1. Click on the # beside DATE column to change from ### to DATE data type
1. To any sheet. It does not matter which. On a sheet, in editing mode, in the upper-left card panel, data sources will be listed.
1. Click on FinalDF, right click and select REPLACE DATA SOURCE
1. In the first dropdown box, select finalDF and in the second drop down box, pick the DF you made.
1. Click okay.
1. Great, you're ready to view all the sheets including our dashboard on the final page.
1. JUST KIDDING. Now, all the custom images we added are referenced with absolute paths in this terrible program. As a result, on each graph our customer header file needs to be added. Browse to any graph and just below the title on the left hand side will be an image. Right click on the image, hit edit and browse to the /images directory in the cloned respository, choose the appropriate header and hit OKAY.  
   a. If the graph is of a comparison betwee Kelowna, Vancouver and Canada, pick the legend with RBG in the name.
   b. If the graph is of all housing types in Kelowna, pick the legend coded BBBB.
   c. If the graph is of all housing types in Vancouver, pick the legend coded GGGG.
   d. If the graph is of all housing types in Kelowna, pick the legend coded RRRR.
   e. If the image is the main header on the dashboard (as seen in video), load PM4-Header.png

_On a professional programming software, absolute paths SHOULD BE AN OPTION THAT WORKS._
Politely, I just want to add Tableau is software, in my humble opinion, for people who can't build a dashboard in excel or Python, and it needs work on user friendliness. There are elements missing that SHOULD EXIST. If you select/highlight a navigation button, hit control+c and then press control+v, it does not make a new navigation button. Instead, it creates some bizzard new sheet with an array list of variables describing the nevigation button.

My initial thoughts ????? Why?! I tried doing it 3 times in disbelief you can't copy a navigation button. Has nobody ever though copying/pasting image, text or nav-button elements on a dashboard, would be desirable/preferable over dragging a new element on? Basic UI failures. The amount of steps we must go through just to get our dashboard to open properly is unacceptable for paid professional software. This would take upwards of 5 minutes before presenting to a client. It feels like Beta software.
