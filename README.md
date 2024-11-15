# CEDA ARD finder

Find ARD in CEDA by searching using their Elasticsearch API.

[The CEDA data portal ARD data](https://catalogue.ceda.ac.uk/uuid/bf9568b558204b81803eeebcc7f529ef)

## Installation and setup

Create virtual env

```
cd cd_ard_finder

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

# Integrating this workflow into another using a python package

```
pip install git+https://github.com/jncc/ceda_ard_finder.git@vx.x.x
```

`vx.x.x` is the version and can be found in [tags](https://github.com/jncc/ceda_ard_finder/tags)

Then import the required tasks into your workflow and use them:

```
...
from ceda_ard_finder import CreateSymlinksFromTextFileList, CreateSymlinksFromFilters

@requires(CreateSymlinksFromFilters)
class GetArdProducts(luigi.Task):
    ...
```

## Setup and running

All search filters are optional except startDate and endDate. See ceda-ard-finder-luigi.cfg.example for more filter examples.

Generate state file with search results:

```
LUIGI_CONFIG_PATH=ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder SearchForProducts --startDate=2021-01-01 --endDate=2021-01-31 --local-scheduler
```

Output results as symlinks to CEDA locations:

```
LUIGI_CONFIG_PATH=ceda_ard_finder/ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder CreateSymlinksFromFilters --startDate=2021-01-01 --endDate=2021-01-31 --local-scheduler
```

Output results as symlinks to CEDA locations (Ingested from a [text file](./inputs.txt.example)):

```
LUIGI_CONFIG_PATH=ceda_ard_finder/ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder CreateSymlinksFromTextFileList --startDate=2021-01-01 --endDate=2021-01-31 --local-scheduler
```

Output results in a text file instead of symlinks.  The date range queried is included in the filename:

```
LUIGI_CONFIG_PATH=ceda_ard_finder/ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder CreateTextFileList --startDate=2021-01-01 --endDate=2021-01-31 --local-scheduler
```
