// Pilot script to extract hourly ERA5 weather for a small batch of CONUS POUS events.
// For each event-hour, this computes county-average weather over the full county polygon
// and exports a CSV with the same event-hour structure used in the Florida workflow.

// ------------------------------------------------------------
// 0) Parameters
// ------------------------------------------------------------
var INPUT_ASSET = 'projects/outage-recovery-forecasting-gc/assets/CONUS_POUS';
var PILOT_N_EVENTS = 20;   // small pilot batch first
var PAD_BEFORE_HOURS = 12;
var PAD_AFTER_HOURS = 12;


// ------------------------------------------------------------
// 1) Load CONUS POUS events
// ------------------------------------------------------------
var eventsRaw = ee.FeatureCollection(INPUT_ASSET);

var events = eventsRaw.map(function (f) {
  f = ee.Feature(f);
  var d = ee.Dictionary(f.toDictionary());

  var countyFips = ee.String(d.get('CountyFIPS'));
  countyFips = ee.Number.parse(countyFips).format('%05d');

  var eventStartStr = ee.String(d.get('event_start'));
  var durationHours = ee.Number.parse(ee.String(d.get('duration_hours')));
  var storm = ee.String(d.get('storm'));

  // Use provided event_id when present; otherwise build a stable fallback.
  var eventId = ee.Algorithms.If(
    d.contains('event_id'),
    ee.String(d.get('event_id')),
    ee.String(countyFips).cat('_').cat(eventStartStr)
  );

  return ee.Feature(null, {
    event_id: eventId,
    CountyFIPS: countyFips,
    event_start: eventStartStr,
    duration_hours: durationHours,
    storm: storm
  });
});

events = events.sort('event_start').limit(PILOT_N_EVENTS);

print({pilot_event_count: events.size()});
print({pilot_events: events.limit(10)});

var eventGeoids = events.aggregate_array('CountyFIPS').distinct();
print({pilot_event_counties: eventGeoids});


// ------------------------------------------------------------
// 2) County geometries for all CONUS event counties
// ------------------------------------------------------------
var allCounties = ee.FeatureCollection('TIGER/2018/Counties')
  .select(['NAME', 'STATEFP', 'GEOID']);

var counties = allCounties
  .filter(ee.Filter.inList('GEOID', eventGeoids));

print({matched_county_count: counties.size()});
print({matched_counties: counties});


// ------------------------------------------------------------
// 3) Helpers
// ------------------------------------------------------------
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

function numericOrNull(x) {
  return ee.Algorithms.If(
    ee.Algorithms.IsEqual(x, null),
    null,
    ee.Number(x)
  );
}


// ------------------------------------------------------------
// 4) Extract hourly weather for each pilot event window
// ------------------------------------------------------------
var eventHourly = events.map(function (evt) {
  evt = ee.Feature(evt);

  var geoid = ee.String(evt.get('CountyFIPS'));
  var eventId = ee.String(evt.get('event_id'));
  var storm = ee.String(evt.get('storm'));
  var eventStartStr = ee.String(evt.get('event_start'));
  var durationHours = ee.Number(evt.get('duration_hours'));
  var eventStart = ee.Date.parse('YYYY-MM-dd HH:mm:ss', eventStartStr);
  var windowStart = eventStart.advance(-PAD_BEFORE_HOURS, 'hour');
  var windowEnd = eventStart.advance(durationHours.add(PAD_AFTER_HOURS), 'hour');

  var county = counties.filter(ee.Filter.eq('GEOID', geoid)).first();
  county = ee.Feature(county);

  var weather = ee.ImageCollection('ECMWF/ERA5/HOURLY')
    .filterDate(windowStart, windowEnd)
    .select([
      'instantaneous_10m_wind_gust',
      'u_component_of_wind_10m',
      'v_component_of_wind_10m',
      'total_precipitation',
      'mean_sea_level_pressure',
      'temperature_2m'
    ])
    .map(addDerivedBands);

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
      gust_mps: numericOrNull(gust),
      wind_speed_mps: numericOrNull(wind),
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

  return hourlyRows;
}).flatten();

print({export_row_count: eventHourly.size()});
print({sample_rows: eventHourly.limit(10)});


// ------------------------------------------------------------
// 5) Coverage summary
// ------------------------------------------------------------
var coverage = events.map(function (evt) {
  evt = ee.Feature(evt);
  var geoid = ee.String(evt.get('CountyFIPS'));
  var hasCounty = counties.filter(ee.Filter.eq('GEOID', geoid)).size();
  return ee.Feature(null, {
    event_id: evt.get('event_id'),
    geoid: geoid,
    has_county_geom: hasCounty
  });
});

print({coverage_summary: coverage});


// ------------------------------------------------------------
// 6) Export pilot batch
// ------------------------------------------------------------
Export.table.toDrive({
  collection: eventHourly,
  description: 'conus_pous_era5_hourly_pilot',
  fileFormat: 'CSV'
});