import requests
import lxml.html as lh
import pandas as pd
import string
import os.path
import sys
import traceback
import pickle
import asyncio
from datetime import datetime
import re

import discord
from discord.ext import commands, tasks

TOKEN = 'YOUR_TOKEN_HERE' # Bot Token

saved_locations = 'saved_locations.csv' # Scraped data location
settings_path = 'server_settings.pkl' # Server setting location
polling_rate = 10 # Rate in which the ACT COVID-19 website is polled

url = 'https://www.covid19.act.gov.au/act-status-and-response/act-covid-19-exposure-locations' # URL to scrape


def parse_url(url):
	"""
		Given a url, scrapes the url for cases and returns them as a pandas DataFrame
	"""
	page = requests.get(url)
	page_text = page.text
	latest_cases = re.search('".*csv"', page.text).group(0)

	df_cases = pd.read_csv(latest_cases[1:-1], names = ['Status', 'Exposure Site', 'Street', 'Suburb', 'State', 'Date', 'Arrival Time', 'Departure Time', 'Contact']).drop(['Status', 'State'], axis = 1) # Remove quote marks from url

	# Correct inconsistent dates (some do not have zeros in front - makes comparisons difficult)
	df_cases['Date'] = df_cases['Date'].apply(lambda x: '0' + x if len(x) > 2 and x[1] == '/' else x)
	return df_cases

class MyClient(commands.Bot):
	server_settings = None
	locations = None

	async def on_ready(self):
		print('Logged on as %s' % self.user)

		# Check if location data file exists, otherwise create it
		if not os.path.isfile(saved_locations):
			parse_url(url).to_csv(saved_locations, index = False)

		# Check if server settings data file exists, otherwise create it
		if not os.path.isfile(settings_path):
			default_settings = dict()

			with open(settings_path, 'wb') as f:
				pickle.dump(default_settings, f, protocol = pickle.HIGHEST_PROTOCOL)

		# Load files

		with open(settings_path, 'rb') as f:
			self.server_settings = pickle.load(f)

		self.locations = pd.read_csv(saved_locations)

		check_for_cases.start()


	async def print_location(self, location_info, channel):
		"""
			Given a location, send a formatted embed to the given channel
		"""
		suburb = location_info['Suburb']
		place = location_info['Exposure Site'].replace('FAQs for schools', '')
		date = location_info['Date']
		arrival = location_info['Arrival Time']
		departure = location_info['Departure Time']
		severity = location_info['Contact']

		if severity == 'Close':
			footer = 'Close contact'
			colour = 0xe74c3c
		elif severity == 'Casual':
			footer = 'Casual contact'
			colour = 0xe67e22
		else:
			footer = 'Monitor for symptoms'
			colour = 0x3498db


		title = '%s' % (place)
		desc = '%s \n %s - %s' % (date, arrival, departure)
		img = 'https://emojis.slackmojis.com/emojis/images/1613270132/12724/among_us_report.png'
		embed = discord.Embed(title = title, description = desc, color = colour, author = 'hi')
		embed.set_author(name = suburb)
		embed.set_thumbnail(url = img)
		embed.set_footer(text = footer)



		await channel.send(embed = embed)


	async def on_message(self, message):
		"""
			Main command checker
		"""
		try:
			if len(message.content.strip()) > 0 and message.content.strip()[0] == '!': # Ignore empty/non-command messages
				command = message.content[1:].strip().split(' ')
				command = [cmd.strip() for cmd in command if len(cmd.strip()) > 0] # Strip whitespace

				if command[0] == 'check':
					if not await self.check_new_cases(url):
						await message.channel.send('No new cases')
				elif command[0] == 'subscribed':
					await self.get_subscribed(message.channel)
				elif command[0] == 'subscribe':
					if len(command) > 1:
						await self.subscribe(message.channel, locations = command[1:])
					else:
						await self.subscribe(message.channel)
				elif command[0] == 'unsubscribe':
					if len(command) > 1:
						await self.unsubscribe(message.channel, locations = command[1:])
					else:
						await self.unsubscribe(message.channel)
		except:
			print(traceback.format_exc())

	async def get_subscribed(self, channel):
		"""
			Return if channel is subscribed to notifications and any specific locations they are subscribed to
		"""
		if int(channel.id) in self.server_settings:
			if len(self.server_settings[int(channel.id)]) > 0:
				await channel.send('This channel is subscribed to notifications in: %s' % ' '.join(self.server_settings[int(channel.id)]))				
			else:
				await channel.send('This channel is subscribed to notifications')
		else:
			await channel.send('This channel is not subscribed to notifications')

	async def subscribe(self, channel, locations = None):
		"""
			Subscribe to notifications and/or only notifications for specific locations
		"""
		if int(channel.id) in self.server_settings and locations is None:
			await channel.send('This channel is already subscribed!')
			return
		elif int(channel.id) not in self.server_settings:
			self.server_settings[int(channel.id)] = []
			await channel.send('This channel is now subscribed to alerts!')

		if locations is not None:
			added_locations = []
			for location in locations:
				if location not in self.server_settings[int(channel.id)]:
					self.server_settings[int(channel.id)].append(location)
					added_locations.append(location)

			if len(added_locations) > 0:
				await channel.send('Added: %s' % ' '.join(added_locations))

		await self.save_settings()

	async def unsubscribe(self, channel, locations = None):
		"""
			Unsubscribe to all notifications or only notifications for specific locations
		"""
		if int(channel.id) not in self.server_settings:
			await channel.send('This channel is not subscribed to alerts!')
			return
		elif locations is None:
			del self.server_settings[int(channel.id)]
			await channel.send('This channel is now unsubscribed')
		else:
			removed_locations = []
			for location in locations:
				if location in self.server_settings[int(channel.id)]:
					self.server_settings[int(channel.id)].remove(location)
					removed_locations.append(location)

			if len(removed_locations) > 0:
				await channel.send('Removed: %s' % ''.join(removed_locations))

		await self.save_settings()
	
	async def save_settings(self):
		"""
			Update server settings file on disk
		"""
		with open(settings_path, 'wb') as f:
				pickle.dump(self.server_settings, f, protocol = pickle.HIGHEST_PROTOCOL)

	async def check_new_cases(self, url, update_if_new = True):
		"""
			Check for new cases and send notifications to subscribed channels
		"""
		print('%s - Checking for new cases' % datetime.now().strftime('%H:%M:%S'))

		prev_locations = self.locations
		updated_locations = parse_url(url)

		print('Previous size: %s - Updated size: %s' % (prev_locations.shape[0], updated_locations.shape[0]))

		new_cases = updated_locations[updated_locations['Contact'] == 4] # Just a cheeky way to get an empty dataframe with the right header
		# Determine new locations
		new_cases = prev_locations.append(updated_locations).drop_duplicates(subset = ['Exposure Site', 'Suburb', 'Date'], keep = False)

		if new_cases.shape[0] == 0:
			print('No new cases found')
			return False

		print('%s new/updated exposure sites scraped' % new_cases.shape[0])

		# Update locations on disk
		if update_if_new:
			self.locations = updated_locations
			updated_locations.to_csv(saved_locations, index = False)

		# Send notifications
		for channel_id in self.server_settings:
			channel = discord.utils.get(self.get_all_channels(), id = channel_id)
			subscribed_locations = [location.replace('_', ' ') for location in self.server_settings[channel_id]] # Change underscores to spaces

			updates = new_cases.copy()

			# Prune to only subscribed locations for relevant channels
			if len(subscribed_locations) > 0:
				updates = new_cases[new_cases['Suburb'].str.strip().isin(subscribed_locations)]

			if updates.shape[0] > 0:
				await channel.send('%s new/updated exposure sites for your subscribed suburbs' % updates.shape[0])

			for _, case in updates.iterrows():
				await self.print_location(case, channel)
				await asyncio.sleep(0.5)

		return True

	# Check for new cases (some checks )
	async def poll_cases(self):
		try:
			if self.server_settings is not None and self.locations is not None:
				await self.check_new_cases(url)		
		except:
			print(traceback.format_exc())

client = MyClient('!')

# Background task to check for cases every `polling_rate` minutes
@tasks.loop(minutes = polling_rate)
async def check_for_cases():
	await client.poll_cases()

client.run(TOKEN)