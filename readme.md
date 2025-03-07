# create a venv

python -m venv venv_sf_drivers
source venv_sf_drivers activate (on Mac)

i have the potentially nasty habit to install my venv in the same folder as my project. So make sure you add your venv* folder to the .gitignore (my specific folder I use above is already added)

# install liibraries
pip install -r requirements.txt

# modify the sfconfig.json with you snowflake details

# run the script. Some remarks
don't run this in production unless throughly tested
the code is 90 % AI generated. The number of prompts needed ..... somehwere in the 20+ range
for now, the final step is not working (as designed) but feel free to add smtp details to this
the suggestion in the script is: Please consider using SQLAlchemy -> will work on that in future versions
it contains debugging information of the first 2 dataframes
the first 2 dataframes (sessions and web scrape) are als in separate code snippets, just as testing efforts before merging the code
