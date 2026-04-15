// gee_era5_hourly_florida_storm_window.js
// ------------------------------------------------------------
// Extract hourly ERA5 weather for all Florida counties over one
// short storm-relevant window.
//
// What this does:
// 1) loads all Florida counties,
// 2) pulls hourly ERA5 weather for a short chosen window,
// 3) derives wind speed from u/v components,
// 4) aggregates weather to county-hour rows,
// 5) previews county boundaries and the first gust field on the map,
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
//
// Notes:
// - This is a proof-of-concept at useful scale.
// - Values are county averages from a gridded reanalysis dataset.
// - Null-safe conversions are used so missing reductions do not crash.
// ------------------------------------------------------------

// Choose a short storm window.
// Adjust this to match a POUS event or known storm period.
var startDate = '2017-09-08';
var endDate   = '2017-09-12';

// Florida counties.
var counties = ee.FeatureCollection('TIGER/2018/Counties')
  .filter(ee.Filter.eq('STATEFP', '12'))
  .select(['NAME', 'STATEFP', 'GEOID']);

print({'Florida county count': counties.size()});
print({'First counties': counties.limit(5)});

// ERA5 hourly weather.
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
  firstImage.select('instantaneous_10m_wind_gust').clip(counties),
  {min: 0, max: 35},
  'First hour gust (m/s)'
);

// Null-safe helper.
function numOrNull(x) {
  return ee.Algorithms.If(ee.Algorithms.IsEqual(x, null), null, ee.Number(x));
}

// County-hour aggregation.
var countyHourly = ee.FeatureCollection(
  weatherDerived.map(function(img) {
    img = ee.Image(img);
    var dt = img.date().format('YYYY-MM-dd HH:mm');

    var reduced = img.reduceRegions({
      collection: counties,
      reducer: ee.Reducer.mean(),
      scale: 27830,
      tileScale: 2,
      maxPixelsPerRegion: 1e8
    }).map(function(f) {
      var gust = f.get('instantaneous_10m_wind_gust');
      var wind = f.get('wind_speed_mps');
      var precip = f.get('total_precipitation');
      var pressure = f.get('mean_sea_level_pressure');
      var temp = f.get('temperature_2m');

      return ee.Feature(null, {
        county: f.get('NAME'),
        geoid: f.get('GEOID'),
        datetime: dt,
        gust_mps: numOrNull(gust),
        wind_speed_mps: numOrNull(wind),
        precip_mm: ee.Algorithms.If(
          ee.Algorithms.IsEqual(precip, null),
          null,
          ee.Number(precip).multiply(1000)
        ),
        pressure_hpa: ee.Algorithms.If(
          ee.Algorithms.IsEqual(pressure, null),
          null,
          ee.Number(pressure).divide(100)
        ),
        temp_c: ee.Algorithms.If(
          ee.Algorithms.IsEqual(temp, null),
          null,
          ee.Number(temp).subtract(273.15)
        )
      });
    });

    return reduced;
  }).flatten()
);

print({'Export row count': countyHourly.size()});
print({'Sample rows': countyHourly.limit(10)});

// Optional sanity-check chart for one county.
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
  description: 'florida_county_weather_era5_hourly_storm_window',
  fileFormat: 'CSV'
});