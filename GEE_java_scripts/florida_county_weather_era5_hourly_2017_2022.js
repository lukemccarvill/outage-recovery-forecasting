// Florida county-level hourly weather extraction for outage recovery work.
//
// What this does:
// 1) loads Florida county boundaries,
// 2) pulls hourly ERA5 weather for the POUS time window,
// 3) derives wind speed from u/v components,
// 4) aggregates weather to county-hour rows,
// 5) previews a weather layer and a time series on the map/chart,
// 6) exports the result as CSV to Google Drive.
//
// Output fields:
// - county
// - geoid
// - datetime
// - gust_mps
// - wind_speed_mps
// - precip_mm
// - pressure_hpa
// - temp_c

// Adjust these to match your POUS/timeseries window.
var startDate = '2017-01-01';
var endDate   = '2022-12-31';

// Florida counties.
var counties = ee.FeatureCollection('TIGER/2018/Counties')
  .filter(ee.Filter.eq('STATEFP', '12'))
  .select(['NAME', 'STATEFP', 'GEOID']);

print('Florida county count: ' + counties.size());
print({'First counties': counties.limit(5)});

// Hourly ERA5 weather.
var weather = ee.ImageCollection('ECMWF/ERA5/HOURLY')
  .filterDate(startDate, endDate)
  .select([
    'instantaneous_10m_wind_gust',
    'u_component_of_wind_10m',
    'v_component_of_wind_10m',
    'total_precipitation',
    'mean_sea_level_pressure',
    'temperature_2m'
  ]);

print({'Weather image count': weather.size()});

// Add derived wind speed band.
function addDerivedBands(img) {
  img = ee.Image(img);

  var windSpeed = img.select('u_component_of_wind_10m')
    .pow(2)
    .add(img.select('v_component_of_wind_10m').pow(2))
    .sqrt()
    .rename('wind_speed_mps');

  return img.addBands(windSpeed).select([
    'instantaneous_10m_wind_gust',
    'wind_speed_mps',
    'total_precipitation',
    'mean_sea_level_pressure',
    'temperature_2m'
  ]);
}

var weatherDerived = weather.map(addDerivedBands);

print({'First image band names': ee.Image(weatherDerived.first()).bandNames()});
print({'First image date': ee.Image(weatherDerived.first()).date()});

// Map preview.
var firstImage = ee.Image(weatherDerived.first());
Map.centerObject(counties, 6);
Map.addLayer(
  counties.style({color: 'ffffff', fillColor: '00000000', width: 1}),
  {},
  'Florida counties'
);
Map.addLayer(
  firstImage.select('instantaneous_10m_wind_gust'),
  {min: 0, max: 35},
  'First hour gust (m/s)'
);

// County-hour aggregation.
var countyHourly = ee.FeatureCollection(
  weatherDerived.map(function(img) {
    img = ee.Image(img);
    var dt = img.date().format('YYYY-MM-dd HH:mm');

    var reduced = img.reduceRegions({
      collection: counties,
      reducer: ee.Reducer.mean(),
      scale: 27830,
      tileScale: 4,
      maxPixelsPerRegion: 1e9
    }).map(function(f) {
      return ee.Feature(null, {
        county: f.get('NAME'),
        geoid: f.get('GEOID'),
        datetime: dt,
        gust_mps: f.get('instantaneous_10m_wind_gust'),
        wind_speed_mps: f.get('wind_speed_mps'),
        precip_mm: ee.Number(f.get('total_precipitation')).multiply(1000),
        pressure_hpa: ee.Number(f.get('mean_sea_level_pressure')).divide(100),
        temp_c: ee.Number(f.get('temperature_2m')).subtract(273.15)
      });
    });

    return reduced;
  }).flatten()
);

print({'Export row count': countyHourly.size()});
print({'Sample rows': countyHourly.limit(10)});

// Simple chart for one county, mainly as a sanity check.
var exampleCounty = ee.Feature(counties.first());

var gustChart = ui.Chart.image.seriesByRegion({
  imageCollection: weatherDerived.select('instantaneous_10m_wind_gust'),
  regions: ee.FeatureCollection([exampleCounty]),
  reducer: ee.Reducer.mean(),
  band: 'instantaneous_10m_wind_gust',
  scale: 27830,
  seriesProperty: 'NAME',
  xProperty: 'system:time_start'
});

gustChart.setOptions({
  title: 'Example Florida county gust time series',
  vAxis: {title: 'Gust (m/s)'},
  lineWidth: 1,
  pointSize: 0
});

print(gustChart);

// Export to Google Drive.
Export.table.toDrive({
  collection: countyHourly,
  description: 'florida_county_weather_era5_hourly_2017_2022',
  fileFormat: 'CSV'
});