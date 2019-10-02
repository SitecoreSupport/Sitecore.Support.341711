using System;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Sitecore.Abstractions;
using Sitecore.Caching;
using Sitecore.Caching.Generics;
using Sitecore.Data;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.Managers;
using Sitecore.DependencyInjection;
using Sitecore.Diagnostics;
using Sitecore.Events;
using Sitecore.Framework.Conditions;
using Sitecore.Globalization;
using Sitecore.Publishing.Diagnostics;
using Sitecore.Publishing.Service.Abstractions.Events;

namespace Sitecore.Support.Publishing.Service.Delivery
{
    public class TargetDataCacheClearHandler
    {
        private readonly string AttemptedToRaiseRemoteEventsButEventArgsInvalid = "Attempted to raise the remote item events at the end of a bulk publish, but the event arguments were not valid.";
        private readonly string CouldNotReflectToFindSqlDataProvider = "Could not reflect to find the SqlDataProvider prefetch cache for clearing at the end of publish jobs.";
        private readonly string CouldNotReflectToFindLanguageCache = "Could not reflect to find the Language cache for clearing at the end of publish jobs.";
        private readonly string NoDatabaseSpecified = "No database name was specified.";
        private readonly string ErrorOccuredClearingTargetDataCache = "There was an error clearing all the target data caches at the end of the publish job";
        private readonly string StartingToClearTargetDataCache = "Starting to clear target data caches for database ";
        private readonly string FinishedClearingTargetCaches = "Finished clearing target data caches for database ";


        private readonly BaseFactory _factory;
        private readonly BaseTranslate _translate;

        private PropertyInfo _prefetchCacheInfo;
        private FieldInfo _languageCacheInfo;


        public TargetDataCacheClearHandler() : this(ServiceLocator.ServiceProvider.GetService<BaseFactory>(), ServiceLocator.ServiceProvider.GetService<BaseTranslate>())
        {
        }

        public TargetDataCacheClearHandler(BaseFactory factory, BaseTranslate translate)
        {
            Condition.Requires(factory, "factory").IsNotNull();
            Condition.Requires(translate, "translate").IsNotNull();

            _factory = factory;
            _translate = translate;
        }

        public void ClearTargetDataCaches(object sender, EventArgs args)
        {
            Condition.Requires(sender, "sender").IsNotNull();
            Condition.Requires(args, "args").IsNotNull();

            Condition.Requires(sender, "sender").IsNotNull();
            Condition.Requires(args, "args").IsNotNull();

            var sitecoreEventArgs = args as SitecoreEventArgs;

            if ((sitecoreEventArgs != null ? sitecoreEventArgs.Parameters : null) == null ||
                !sitecoreEventArgs.Parameters.Any() ||
                !(sitecoreEventArgs.Parameters[0] is TargetDataCacheClearEventArgs))
            {
                // TODO: Use better logging
                Log.Error(AttemptedToRaiseRemoteEventsButEventArgsInvalid, this);
                return;
            }

            var eventArgs = sitecoreEventArgs.Parameters[0] as TargetDataCacheClearEventArgs;

            // Reflect to find expected private cache properties
            _prefetchCacheInfo = typeof(SqlDataProvider).GetProperty("PrefetchCache", BindingFlags.NonPublic | BindingFlags.Instance);
            if (_prefetchCacheInfo == null)
            {
                PublishingLog.Warn(CouldNotReflectToFindSqlDataProvider);
            }

            _languageCacheInfo = typeof(LanguageProvider).GetField("_languageCache", BindingFlags.NonPublic | BindingFlags.Instance);
            if (_languageCacheInfo == null)
            {
                PublishingLog.Warn(CouldNotReflectToFindLanguageCache);
            }

            var targetDb = _factory.GetDatabase(eventArgs.EventData.DatabaseName);
            if (targetDb == null)
            {
                throw new ArgumentOutOfRangeException("targetDatabase", NoDatabaseSpecified);
            }

            try
            {
                ClearCaches(targetDb);
            }
            catch (Exception ex)
            {
                PublishingLog.Error(ErrorOccuredClearingTargetDataCache, ex);
                throw;
            }
        }

        private void ClearCaches(Database database)
        {
            PublishingLog.Info(StartingToClearTargetDataCache + database.Name);

            database.Caches.DataCache.Clear();
            database.Caches.ItemCache.Clear();
            database.Caches.ItemPathsCache.Clear();
            database.Caches.IsFallbackValidCache.Clear();

            var fallbackFieldValuesCache = database.Caches.FallbackFieldValuesCache as CustomCache<LanguageFallbackFieldValuesCacheKey>;

            if (fallbackFieldValuesCache != null)
            {
                fallbackFieldValuesCache.Clear();
            }

            database.Caches.PathCache.Clear();
            database.Caches.StandardValuesCache.Clear();

            // SqlDataProvider
            if (_prefetchCacheInfo != null)
            {
                foreach (var cache in database
                    .GetDataProviders()
                    .Where(p => p is SqlDataProvider)
                    .Select(p => _prefetchCacheInfo.GetValue(p))
                    .Where(c => c is ICacheInfo))
                {
                    ((ICacheInfo) cache).Clear();
                }
            }

            // deviceItemsCache
            var cacheManager = ServiceLocator.ServiceProvider.GetRequiredService<BaseCacheManager>();

            // access result cache
            cacheManager.ClearAccessResultCache();

            // deviceItemsCache
            cacheManager.GetDeviceItemsCache().Clear();

            // PlaceholderCache
            var placeholderManager = ServiceLocator.ServiceProvider.GetRequiredService<BasePlaceholderCacheManager>();
            placeholderManager.GetPlaceholderCache(database.Name).Reload();

            // RuleCache
            var ruleFactory = ServiceLocator.ServiceProvider.GetRequiredService<BaseRuleFactory>();
            ruleFactory.InvalidateCache();

            // Language Provider
            if (_languageCacheInfo != null)
            {
                var languageProvider = GetLanguageProvider();
                var languageCache = (ICacheInfo) _languageCacheInfo.GetValue(languageProvider);
                languageCache.Clear();
            }

            // TemplateEngine
            database.Engines.TemplateEngine.Reset();
           
            // Translation
            _translate.ResetCache();

            PublishingLog.Info(FinishedClearingTargetCaches + database.Name);
        }

        private LanguageProvider GetLanguageProvider()
        {
            var lazyResetableLanguageCacheFieldInfo = typeof(LanguageManager).GetField("LanguageProvider", BindingFlags.NonPublic | BindingFlags.Static);
            if (lazyResetableLanguageCacheFieldInfo != null)
            {
                var lazyResetableLanguageCache = (LazyResetable<LanguageProvider>) lazyResetableLanguageCacheFieldInfo.GetValue(null);
                if (lazyResetableLanguageCache != null)
                {
                    return lazyResetableLanguageCache.Value;
                }
            }

            return null;
        }
    }
}