// gee_era5_hourly_florida_event_windows_toy_tft.js
// ------------------------------------------------------------
// Extract hourly ERA5 weather for a small Florida-only set of
// POUS event windows, for use in a toy TFT test.
//
// What this does:
// 1) defines a small list of Florida outage events,
// 2) treats county FIPS as 5-digit strings,
// 3) builds one weather window per event:
//      [event_start - 12h, event_start + duration_hours + 12h]
// 4) extracts hourly ERA5 weather for the matching county only,
// 5) derives wind speed from u/v components,
// 6) exports one row per event-hour.
//
// Output fields:
// - event_id
// - storm
// - geoid
// - county
// - datetime
// - event_start
// - duration_hours
// - gust_mps
// - wind_speed_mps
// - precip_mm
// - pressure_hpa
// - temp_c
//
// Notes:
// - This is a compact proof-of-concept for the toy TFT workflow.
// - County codes are stored as strings with leading zeros preserved.
// - Null-safe conversions are used so missing reductions do not crash.
// ------------------------------------------------------------


// ------------------------------------------------------------
// 1) Toy Florida-only event list
//    Chosen from your POUS rows around Irma-like storm timing.
//    GEOIDs are 5-digit strings on purpose.
// ------------------------------------------------------------
var events = ee.FeatureCollection([
  ee.Feature(null, {
    event_id: 'event_001',
    geoid: '12075',
    event_start: '2017-09-09 22:00',
    duration_hours: 190,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_002',
    geoid: '12067',
    event_start: '2017-09-09 20:00',
    duration_hours: 191,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_003',
    geoid: '12079',
    event_start: '2017-09-10 02:00',
    duration_hours: 124,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_004',
    geoid: '12011',
    event_start: '2017-09-10 06:00',
    duration_hours: 178,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_005',
    geoid: '12086',
    event_start: '2017-09-10 06:00',
    duration_hours: 210,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_006',
    geoid: '12125',
    event_start: '2017-09-10 11:00',
    duration_hours: 133,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_007',
    geoid: '12111',
    event_start: '2017-09-10 14:00',
    duration_hours: 131,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_008',
    geoid: '12099',
    event_start: '2017-09-10 14:00',
    duration_hours: 147,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_009',
    geoid: '12021',
    event_start: '2017-09-10 14:00',
    duration_hours: 263,
    storm: '2017242N16333'
  }),
  ee.Feature(null, {
    event_id: 'event_010',
    geoid: '12093',
    event_start: '2017-09-10 16:00',
    duration_hours: 175,
    storm: '2017242N16333'
  })
]);

print({'Toy event count': events.size()});
print({'Toy events': events});


// ------------------------------------------------------------
// 2) County geometries from TIGER
// ------------------------------------------------------------
var allCounties = ee.FeatureCollection('TIGER/2018/Counties')
  .select(['NAME', 'STATEFP', 'GEOID']);

var eventGeoids = ee.List(events.aggregate_array('geoid')).distinct();

var counties = allCounties
  .filter(ee.Filter.eq('STATEFP', '12'))
  .filter(ee.Filter.inList('GEOID', eventGeoids));

print({'Matched county count': counties.size()});
print({'Matched counties': counties});

Map.centerObject(counties, 6);
Map.addLayer(
  counties.style({color: 'ffffff', fillColor: '00000000', width: 2}),
  {},
  'Toy TFT counties'
);


// ------------------------------------------------------------
// 3) Helpers
// ------------------------------------------------------------
function numOrNull(x) {
  return ee.Algorithms.If(
    ee.Algorithms.IsEqual(x, null),
    null,
    ee.Number(x)
  );
}

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


// ------------------------------------------------------------
// 4) Preview the first event on the map
// ------------------------------------------------------------
var firstEvent = ee.Feature(events.first());
var firstEventCounty = counties.filter(ee.Filter.eq('GEOID', firstEvent.get('geoid')));
var firstEventStart = ee.Date.parse('YYYY-MM-dd HH:mm', firstEvent.getString('event_start'));

var firstImage = ee.ImageCollection('ECMWF/ERA5/HOURLY')
  .filterDate(firstEventStart, firstEventStart.advance(1, 'hour'))
  .select([
    'instantaneous_10m_wind_gust',
    'u_component_of_wind_10m',
    'v_component_of_wind_10m',
    'total_precipitation',
    'mean_sea_level_pressure',
    'temperature_2m'
  ])
  .map(addDerivedBands)
  .first();

Map.addLayer(
  ee.Image(firstImage).select('instantaneous_10m_wind_gust').clip(firstEventCounty),
  {min: 0, max: 35},
  'First event first-hour gust (m/s)'
);


// ------------------------------------------------------------
// 5) Extract hourly weather for each event window
// ------------------------------------------------------------
var eventHourly = events.map(function (evt) {
  evt = ee.Feature(evt);
  var geoid = evt.getString('geoid');
  var eventId = evt.getString('event_id');
  var storm = evt.getString('storm');
  var eventStartStr = evt.getString('event_start');
  var durationHours = ee.Number(evt.get('duration_hours'));
  var eventStart = ee.Date.parse('YYYY-MM-dd HH:mm', eventStartStr);
  var windowStart = eventStart.advance(-12, 'hour');
  var windowEnd = eventStart.advance(durationHours.add(12), 'hour');
  var county = counties.filter(ee.Filter.eq('GEOID', geoid)).first();
  county = ee.Feature(county);
  var weather = ee.ImageCollection('ECMWF/ERA5/HOURLY').filterDate(windowStart, windowEnd).select([
    'instantaneous_10m_wind_gust',
    'u_component_of_wind_10m',
    'v_component_of_wind_10m',
    'total_precipitation',
    'mean_sea_level_pressure',
    'temperature_2m'
  ]).map(addDerivedBands);
  var hourlyRows = weather.map(function (img) {
    img = ee.Image(img);
    var stats = img.reduceRegion({
      reducer: ee.Reducer.mean(),
      geometry: county.geometry(),
      scale: 27830,
      tileScale: 2,
      maxPixels: 100000000
    });
    var gust = stats.get('instantaneous_10m_wind_gust');
    var wind = stats.get('wind_speed_mps');
    var precip = stats.get('total_precipitation');
    var pressure = stats.get('mean_sea_level_pressure');
    var temp = stats.get('temperature_2m');
    return ee.Feature(null, {
      event_id: eventId,
      storm: storm,
      geoid: geoid,
      county: county.get('NAME'),
      datetime: img.date().format('YYYY-MM-dd HH:mm'),
      event_start: eventStartStr,
      duration_hours: durationHours,
      gust_mps: numOrNull(gust),
      wind_speed_mps: numOrNull(wind),
      precip_mm: ee.Algorithms.If(ee.Algorithms.IsEqual(precip, null), null, ee.Number(precip).multiply(1000)),
      pressure_hpa: ee.Algorithms.If(ee.Algorithms.IsEqual(pressure, null), null, ee.Number(pressure).divide(100)),
      temp_c: ee.Algorithms.If(ee.Algorithms.IsEqual(temp, null), null, ee.Number(temp).subtract(273.15))
    });
  });
  return hourlyRows;
}).flatten();

print({'Export row count': eventHourly.size()});
print({'Sample rows': eventHourly.limit(10)});


// ------------------------------------------------------------
// 6) Export
// ------------------------------------------------------------
Export.table.toDrive({
  collection: eventHourly,
  description: 'toy_tft_florida_event_weather_era5_hourly',
  fileFormat: 'CSV'
});