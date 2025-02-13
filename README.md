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

Installing a specific version:

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

Add to `requirements.txt` of your workflow

```
...
ceda_ard_finder @ git+https://git@github.com/jncc/ceda_ard_finder.git@v0.0.1
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
LUIGI_CONFIG_PATH=ceda_ard_finder/ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder CreateSymlinksFromTextFileList --local-scheduler
```

Output results in a text file instead of symlinks.  The date range queried is included in the filename:

```
LUIGI_CONFIG_PATH=ceda_ard_finder/ceda-ard-finder-luigi.cfg PYTHONPATH='.' luigi --module ceda_ard_finder CreateTextFileList --startDate=2021-01-01 --endDate=2021-01-31 --local-scheduler
```

## Misc Info

- `spatialOperator` is a [required parameter](https://github.com/jncc/ceda_ard_finder/blob/a1834fc36ae756cbcf86ffae053fbb06832023da/ceda_ard_finder/SearchForProducts.py#L25). However, it will not be processed unless `wkt` is [also supplied](https://github.com/jncc/ceda_ard_finder/blob/a1834fc36ae756cbcf86ffae053fbb06832023da/ceda_ard_finder/SearchForProducts.py#L59-L60). If you don't intend to use `spatialOperator`, you can just set it to `intersects`. You do not have to supply a `wkt` parameter.

- Do not supply a `ardFilter` without a `satelliteFilter` parmeter to the [SearchForProducts](https://github.com/jncc/ceda_ard_finder/blob/a1834fc36ae756cbcf86ffae053fbb06832023da/ceda_ard_finder/SearchForProducts.py#L77-L81) task unless searching for Sentinel-1/2 **ARD** products because the task will derive the `satelliteFilter` from the `ardFilter` if it is not supplied and this is only compatible with Sentinel-1/2 **ARD** product searches.
