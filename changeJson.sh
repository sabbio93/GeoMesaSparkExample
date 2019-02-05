#!/bin/bash
cat ./data/neighborhoods.geojson | jq -c '.features | .[]'> 2.json
 
