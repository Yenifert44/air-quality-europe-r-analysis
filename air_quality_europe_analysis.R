############################################################
# PROJECT: Air quality comparison in European cities
# Cities: Barcelona, Lisbon, Amsterdam
# Author: Yenifert Lipa
# Description:
# This script downloads air quality data from OpenAQ and weather data
# from Open-Meteo. It compares PM2.5 and NO2 levels across three
# European cities and adjusts pollutant values by meteorological variables.
############################################################

#########################
# 0. PACKAGES
#########################

required_packages <- c(
  "httr", "jsonlite", "dplyr", "purrr", "tidyr",
  "readr", "lubridate", "ggplot2", "rstatix", "patchwork"
)

new_packages <- required_packages[
  !required_packages %in% installed.packages()[, "Package"]
]

if (length(new_packages)) {
  install.packages(new_packages, dependencies = TRUE)
}

invisible(lapply(required_packages, library, character.only = TRUE))


#########################
# 1. CONFIGURATION
#########################

# IMPORTANT:
# Do not write your OpenAQ API key inside this script.
# Before running the script, set the API key in your local R console:
# Sys.setenv(OPENAQ_KEY = "YOUR_API_KEY")

api <- "https://api.openaq.org/v3"

key <- trimws(Sys.getenv("OPENAQ_KEY"))

if (!nzchar(key)) {
  stop("Missing OpenAQ API key. Set OPENAQ_KEY before running the script.")
}

# Cities and search radius
cities <- tibble::tibble(
  city = c("Barcelona", "Lisboa", "Amsterdam"),
  lat = c(41.387, 38.722, 52.372),
  lon = c(2.170, -9.139, 4.894),
  radius = c(15000, 15000, 15000),
  max_stations = c(9, 9, 9)
)

# Date range: last 120 days
date_to <- Sys.Date()
date_from <- date_to - 120


#########################
# 2. API REQUEST HELPERS
#########################

# GET request with authentication fallback
auth_get <- function(url, query = NULL, key) {
  response_1 <- httr::GET(
    url,
    query = query,
    httr::add_headers("X-API-Key" = key)
  )
  
  if (httr::status_code(response_1) != 401) {
    return(response_1)
  }
  
  response_2 <- httr::GET(
    url,
    query = query,
    httr::add_headers(Authorization = paste("Bearer", key))
  )
  
  return(response_2)
}

# GET request with retry protection for rate limits
throttled_get <- function(url, query = NULL, key, max_tries = 6, wait = 1) {
  for (i in seq_len(max_tries)) {
    
    response <- httr::GET(
      url,
      query = query,
      httr::add_headers("X-API-Key" = key)
    )
    
    status <- httr::status_code(response)
    
    if (status == 429) {
      pause <- wait * (2^(i - 1)) + runif(1, 0, 0.5)
      message(sprintf("Rate limit reached. Waiting %.1f seconds...", pause))
      Sys.sleep(pause)
      next
    }
    
    if (status >= 400) {
      response_bearer <- httr::GET(
        url,
        query = query,
        httr::add_headers(Authorization = paste("Bearer", key))
      )
      
      status_bearer <- httr::status_code(response_bearer)
      
      if (status_bearer == 429) {
        pause <- wait * (2^(i - 1)) + runif(1, 0, 0.5)
        message(sprintf("Rate limit reached with Bearer. Waiting %.1f seconds...", pause))
        Sys.sleep(pause)
        next
      }
      
      if (status_bearer < 400) {
        return(response_bearer)
      }
      
      stop(paste("HTTP error", status_bearer, "in", url))
    }
    
    return(response)
  }
  
  stop("Too many attempts due to rate limit.")
}


#########################
# 3. OPENAQ FUNCTIONS
#########################

# Get air quality locations near each city
get_locations_by_coords <- function(lat, lon, radius, limit = 200) {
  url <- sprintf("%s/locations", api)
  
  response <- auth_get(
    url,
    query = list(
      coordinates = paste0(lat, ",", lon),
      radius = radius,
      limit = limit
    ),
    key = key
  )
  
  status <- httr::status_code(response)
  
  if (status >= 400) {
    cat(
      "Error in locations request. HTTP", status, "\n",
      httr::content(response, "text", encoding = "UTF-8"), "\n"
    )
    stop("Locations request failed.")
  }
  
  output <- jsonlite::fromJSON(
    httr::content(response, "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (is.null(output$results) || length(output$results) == 0) {
    return(tibble::tibble())
  }
  
  tibble::as_tibble(output$results) |>
    dplyr::transmute(
      location_id = id,
      location_name = name
    )
}

# Get sensors for each location
get_sensors_for_location <- function(location_id) {
  url <- sprintf("%s/locations/%s/sensors", api, location_id)
  
  response <- auth_get(url, key = key)
  status <- httr::status_code(response)
  
  if (status >= 400) {
    cat(
      "Error in sensors request. HTTP", status,
      "Location:", location_id, "\n",
      httr::content(response, "text", encoding = "UTF-8"), "\n"
    )
    return(tibble::tibble())
  }
  
  output <- jsonlite::fromJSON(
    httr::content(response, "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (is.null(output$results) || length(output$results) == 0) {
    return(tibble::tibble())
  }
  
  sensors <- tibble::as_tibble(output$results)
  sensors$location_id <- location_id
  
  return(sensors)
}

# Get daily values for each sensor
get_days_for_sensor <- function(sensor_id, date_from, date_to) {
  url <- sprintf("%s/sensors/%s/days", api, sensor_id)
  
  response <- throttled_get(
    url,
    query = list(
      date_from = paste0(date_from, "T00:00:00Z"),
      date_to = paste0(date_to, "T23:59:59Z"),
      limit = 1000
    ),
    key = key
  )
  
  output <- jsonlite::fromJSON(
    httr::content(response, "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (is.null(output$results) || NROW(output$results) == 0) {
    return(
      tibble::tibble(
        sensor_id = sensor_id,
        date = as.Date(character()),
        value = numeric()
      )
    )
  }
  
  tibble::tibble(
    sensor_id = sensor_id,
    date = as.Date(substr(output$results$period.datetimeFrom.utc, 1, 10)),
    value = output$results$value
  )
}


#########################
# 4. LOCATIONS AND SENSORS
#########################

locations_all <- cities |>
  dplyr::rowwise() |>
  dplyr::mutate(
    locations = list(get_locations_by_coords(lat, lon, radius))
  ) |>
  dplyr::ungroup() |>
  tidyr::unnest(locations)

if (nrow(locations_all) == 0) {
  stop("No air quality locations found for the selected cities.")
}

# Limit number of stations per city
locations_all <- locations_all |>
  dplyr::group_by(city) |>
  dplyr::mutate(station_index = dplyr::row_number()) |>
  dplyr::filter(station_index <= max_stations) |>
  dplyr::ungroup() |>
  dplyr::select(-station_index)

# Download sensors
sensors_raw <- purrr::map_dfr(
  locations_all$location_id,
  get_sensors_for_location
)

# Normalize parameter name column
if ("parameter.name" %in% names(sensors_raw)) {
  sensors <- dplyr::rename(sensors_raw, parameter_name = `parameter.name`)
} else if ("parameter" %in% names(sensors_raw) &&
           "name" %in% names(sensors_raw$parameter)) {
  sensors <- sensors_raw |>
    dplyr::mutate(parameter_name = parameter$name)
} else {
  sensors <- sensors_raw
  
  if (!"parameter_name" %in% names(sensors)) {
    stop("Parameter name column was not found in sensors data.")
  }
}

# Keep PM2.5 and NO2 sensors only
sensors <- sensors |>
  dplyr::left_join(
    locations_all |>
      dplyr::select(city, location_id),
    by = "location_id"
  ) |>
  dplyr::filter(tolower(parameter_name) %in% c("pm25", "no2")) |>
  dplyr::select(
    sensor_id = id,
    parameter_name,
    location_id,
    city
  )

if (nrow(sensors) == 0) {
  stop("No PM2.5 or NO2 sensors found in the selected locations.")
}

readr::write_csv(sensors, "sensors_pm25_no2_by_city.csv")


#########################
# 5. AIR QUALITY DATA DOWNLOAD
#########################

sensor_ids <- unique(sensors$sensor_id)

chunk_size <- 8
chunks <- split(sensor_ids, ceiling(seq_along(sensor_ids) / chunk_size))

daily_list <- list()

for (i in seq_along(chunks)) {
  message(sprintf("Downloading sensor batch %d/%d", i, length(chunks)))
  
  if (i > 1) {
    Sys.sleep(2)
  }
  
  daily_list[[i]] <- purrr::map_dfr(
    chunks[[i]],
    get_days_for_sensor,
    date_from = date_from,
    date_to = date_to
  )
}

daily_all <- dplyr::bind_rows(daily_list)

# Aggregate by city, pollutant and date
daily_city <- daily_all |>
  dplyr::left_join(sensors, by = "sensor_id") |>
  dplyr::group_by(city, parameter_name, date) |>
  dplyr::summarise(
    value = mean(value, na.rm = TRUE),
    .groups = "drop"
  )

readr::write_csv(daily_city, "air_daily_cities_120d.csv")
message("Saved air_daily_cities_120d.csv")


#########################
# 6. WEATHER DATA FROM OPEN-METEO
#########################

get_meteo <- function(lat, lon, city, start_date, end_date) {
  url <- "https://archive-api.open-meteo.com/v1/era5"
  
  response <- httr::GET(
    url,
    query = list(
      latitude = lat,
      longitude = lon,
      start_date = start_date,
      end_date = end_date,
      daily = "temperature_2m_mean,relative_humidity_2m_mean,wind_speed_10m_max",
      timezone = "UTC"
    )
  )
  
  httr::stop_for_status(response)
  
  output <- jsonlite::fromJSON(
    httr::content(response, "text", encoding = "UTF-8")
  )
  
  tibble::tibble(
    date = as.Date(output$daily$time),
    tmean = output$daily$temperature_2m_mean,
    rh = output$daily$relative_humidity_2m_mean,
    wind = output$daily$wind_speed_10m_max,
    city = city
  )
}

meteo_list <- list()

for (i in seq_len(nrow(cities))) {
  this_city <- cities$city[i]
  
  message("Downloading weather data for: ", this_city)
  
  meteo_list[[i]] <- get_meteo(
    lat = cities$lat[i],
    lon = cities$lon[i],
    city = this_city,
    start_date = as.character(date_from),
    end_date = as.character(date_to)
  )
}

meteo_data <- dplyr::bind_rows(meteo_list)

meteo_summary <- meteo_data |>
  dplyr::group_by(city) |>
  dplyr::summarise(
    first_day = min(date),
    last_day = max(date),
    total_days = dplyr::n(),
    .groups = "drop"
  )

print(meteo_summary)


#########################
# 7. MERGE AIR QUALITY AND WEATHER DATA
#########################

merged_data <- daily_city |>
  dplyr::left_join(meteo_data, by = c("city", "date"))

readr::write_csv(merged_data, "air_daily_with_meteo.csv")


#########################
# 8. METEOROLOGICAL ADJUSTMENT
#########################

# The model adjusts pollutant values by temperature, relative humidity and wind.
# Residuals are used to compare cities after removing part of the weather effect.

model_data <- merged_data |>
  tidyr::drop_na(value, tmean, rh, wind)

groups <- model_data |>
  dplyr::group_by(city, parameter_name) |>
  dplyr::group_split()

residuals_df <- purrr::map_dfr(
  groups,
  function(data_group) {
    
    if (nrow(data_group) < 5) {
      return(
        tibble::tibble(
          date = data_group$date,
          city = data_group$city[1],
          parameter_name = data_group$parameter_name[1],
          residuals = NA_real_
        )
      )
    }
    
    fit <- lm(value ~ tmean + rh + wind, data = data_group)
    
    tibble::tibble(
      date = data_group$date,
      city = data_group$city,
      parameter_name = data_group$parameter_name,
      residuals = resid(fit)
    )
  }
) |>
  dplyr::ungroup()

# Keep only dates available for the three cities
residuals_sync <- residuals_df |>
  tidyr::drop_na(residuals) |>
  dplyr::group_by(date, parameter_name) |>
  dplyr::filter(dplyr::n_distinct(city) == 3) |>
  dplyr::ungroup()

readr::write_csv(residuals_df, "air_daily_residuals.csv")


#########################
# 9. STATISTICAL ANALYSIS
#########################

analysis_data <- daily_city |>
  dplyr::mutate(parameter_name = tolower(parameter_name)) |>
  dplyr::filter(parameter_name %in% c("pm25", "no2")) |>
  tidyr::drop_na(value, city, parameter_name)

# 9.1 Kruskal-Wallis test on raw values
kruskal_raw <- analysis_data |>
  dplyr::group_by(parameter_name) |>
  rstatix::kruskal_test(value ~ city) |>
  dplyr::ungroup()

print(kruskal_raw)
readr::write_csv(kruskal_raw, "kruskal_results_raw.csv")

# 9.2 Kruskal-Wallis test on meteorologically adjusted residuals
kruskal_residuals <- residuals_sync |>
  dplyr::group_by(parameter_name) |>
  rstatix::kruskal_test(residuals ~ city) |>
  dplyr::ungroup()

print(kruskal_residuals)
readr::write_csv(kruskal_residuals, "kruskal_results_residuals.csv")

# 9.3 Dunn post-hoc test on residuals
dunn_residuals <- residuals_sync |>
  dplyr::group_by(parameter_name) |>
  rstatix::dunn_test(residuals ~ city, p.adjust.method = "BH") |>
  dplyr::ungroup()

print(dunn_residuals)
readr::write_csv(dunn_residuals, "dunn_posthoc_residuals.csv")

# 9.4 Spearman correlation between PM2.5 and NO2 by city
wide_data <- analysis_data |>
  dplyr::select(city, date, parameter_name, value) |>
  dplyr::distinct() |>
  tidyr::pivot_wider(
    names_from = parameter_name,
    values_from = value
  )

spearman_by_city <- wide_data |>
  dplyr::group_by(city) |>
  dplyr::summarise(
    n = sum(!is.na(pm25) & !is.na(no2)),
    rho = suppressWarnings(
      cor(pm25, no2, method = "spearman", use = "complete.obs")
    ),
    p_value = suppressWarnings(
      cor.test(pm25, no2, method = "spearman")$p.value
    ),
    .groups = "drop"
  )

print(spearman_by_city)
readr::write_csv(spearman_by_city, "spearman_by_city.csv")


#########################
# 10. VISUAL STYLE
#########################

theme_clean_portfolio <- function(base_size = 11) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "#F9F9F9", color = NA),
      panel.background = ggplot2::element_rect(fill = "#FFFFFF", color = NA),
      panel.grid.major = ggplot2::element_line(color = "#C9CED3"),
      panel.grid.minor = ggplot2::element_blank(),
      plot.title = ggplot2::element_text(
        face = "bold",
        size = base_size + 3,
        color = "#1D3557"
      ),
      plot.subtitle = ggplot2::element_text(
        size = base_size - 1,
        color = "#A4B494"
      ),
      axis.title = ggplot2::element_text(color = "#1D3557"),
      axis.text = ggplot2::element_text(color = "#1D3557"),
      legend.title = ggplot2::element_text(color = "#1D3557"),
      legend.text = ggplot2::element_text(color = "#1D3557"),
      plot.caption = ggplot2::element_text(
        color = "#A4B494",
        size = base_size - 3
      ),
      plot.margin = ggplot2::margin(12, 12, 12, 12)
    )
}

city_colors <- c(
  "Barcelona" = "#F4A261",
  "Lisboa" = "#2A9D8F",
  "Amsterdam" = "#E9C46A"
)


#########################
# 11. PLOTS: RAW VALUES
#########################

pm25_raw <- daily_city |>
  dplyr::filter(tolower(parameter_name) == "pm25")

no2_raw <- daily_city |>
  dplyr::filter(tolower(parameter_name) == "no2")

plot_pm25_raw <- ggplot2::ggplot(
  pm25_raw,
  ggplot2::aes(x = date, y = value, color = city)
) +
  ggplot2::geom_line(linewidth = 1) +
  ggplot2::scale_color_manual(values = city_colors, name = "City") +
  ggplot2::labs(
    title = "PM2.5: daily concentration trends",
    subtitle = "Raw daily concentrations over the last 120 days.",
    x = NULL,
    y = "PM2.5 concentration (µg/m³)",
    caption = "Source: OpenAQ"
  ) +
  theme_clean_portfolio() +
  ggplot2::theme(legend.position = "right")

plot_no2_raw <- ggplot2::ggplot(
  no2_raw,
  ggplot2::aes(x = city, y = value, fill = city)
) +
  ggplot2::geom_boxplot(alpha = 0.9, color = "#1D3557") +
  ggplot2::scale_fill_manual(values = city_colors, name = "City") +
  ggplot2::labs(
    title = "NO2: distribution of raw concentrations",
    subtitle = "Comparison of daily concentration distributions before weather adjustment.",
    x = NULL,
    y = "NO2 concentration",
    caption = "Source: OpenAQ"
  ) +
  theme_clean_portfolio() +
  ggplot2::theme(legend.position = "right")

combined_raw_plot <- plot_pm25_raw / patchwork::plot_spacer() / plot_no2_raw +
  patchwork::plot_layout(heights = c(1, 0.08, 1)) +
  patchwork::plot_annotation(
    caption = paste(
      "Raw pollutant levels may suggest differences between cities.",
      "Weather adjustment helps evaluate whether part of this pattern is explained by meteorological conditions."
    )
  )

print(combined_raw_plot)

ggplot2::ggsave(
  "comparative_PM25_NO2_LinkedIn.png",
  combined_raw_plot,
  width = 10,
  height = 5.6,
  units = "in",
  dpi = 300,
  device = "png"
)

ggplot2::ggsave(
  "comparative_PM25_NO2_Instagram_square.png",
  combined_raw_plot,
  width = 6,
  height = 6,
  units = "in",
  dpi = 300,
  device = "png"
)


#########################
# 12. PLOTS: WEATHER-ADJUSTED RESIDUALS
#########################

pm25_residuals <- residuals_sync |>
  dplyr::filter(tolower(parameter_name) == "pm25")

no2_residuals <- residuals_sync |>
  dplyr::filter(tolower(parameter_name) == "no2")

plot_pm25_residuals <- ggplot2::ggplot(
  pm25_residuals,
  ggplot2::aes(x = city, y = residuals, fill = city)
) +
  ggplot2::geom_boxplot(alpha = 0.9, color = "#1D3557") +
  ggplot2::scale_fill_manual(values = city_colors, name = "City") +
  ggplot2::labs(
    title = "PM2.5 adjusted by weather conditions",
    subtitle = "Residuals after accounting for temperature, humidity and wind.",
    x = NULL,
    y = "Residuals",
    caption = "Kruskal-Wallis test on residuals · Source: OpenAQ and Open-Meteo"
  ) +
  theme_clean_portfolio(base_size = 12) +
  ggplot2::theme(legend.position = "right")

print(plot_pm25_residuals)

ggplot2::ggsave(
  "PM25_residuals_LinkedIn.png",
  plot_pm25_residuals,
  width = 9,
  height = 5,
  units = "in",
  dpi = 300
)

ggplot2::ggsave(
  "PM25_residuals_Instagram.png",
  plot_pm25_residuals,
  width = 6,
  height = 7,
  units = "in",
  dpi = 300
)

plot_no2_residuals <- ggplot2::ggplot(
  no2_residuals,
  ggplot2::aes(x = city, y = residuals, fill = city)
) +
  ggplot2::geom_boxplot(alpha = 0.9, color = "#1D3557") +
  ggplot2::scale_fill_manual(values = city_colors, name = "City") +
  ggplot2::labs(
    title = "NO2 adjusted by weather conditions",
    subtitle = "Residuals after accounting for temperature, humidity and wind.",
    x = NULL,
    y = "Residuals",
    caption = "Kruskal-Wallis test on residuals · Source: OpenAQ and Open-Meteo"
  ) +
  theme_clean_portfolio(base_size = 12) +
  ggplot2::theme(legend.position = "right")

print(plot_no2_residuals)

ggplot2::ggsave(
  "NO2_residuals_LinkedIn.png",
  plot_no2_residuals,
  width = 9,
  height = 5,
  units = "in",
  dpi = 300
)

ggplot2::ggsave(
  "NO2_residuals_Instagram.png",
  plot_no2_residuals,
  width = 6,
  height = 7,
  units = "in",
  dpi = 300
)

message("Script completed. Check the CSV and PNG files saved in your working directory.")

