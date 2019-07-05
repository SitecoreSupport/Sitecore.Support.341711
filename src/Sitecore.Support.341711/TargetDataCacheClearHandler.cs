using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.DependencyInjection;

using Sitecore.Abstractions;
using Sitecore.Caching;
using Sitecore.Caching.Generics;
using Sitecore.Collections;
using Sitecore.Data;
using Sitecore.Data.Managers;
using Sitecore.Data.Proxies;
using Sitecore.DependencyInjection;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.Events.Hooks;
using Sitecore.Publishing.Diagnostics;
using Sitecore.Publishing.Service.Events;

namespace Sitecore.Support.Publishing.Service.Delivery
{
    using System.Reflection;

    using Sitecore.Data.DataProviders.Sql;
    using Sitecore.Events;
    using Sitecore.Framework.Conditions;
    using Sitecore.Publishing.Service.Abstractions.Events;

    public class TargetDataCacheClearHandler
    {
        private readonly IFactory _factory;
        private readonly ITranslate _translate;

        private PropertyInfo _prefetchCacheInfo;
        private FieldInfo _languageCacheInfo;

        public TargetDataCacheClearHandler() : this(new FactoryWrapper(), new TranslateWrapper())
        {
        }

        public TargetDataCacheClearHandler(IFactory factory, ITranslate translate)
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
                Sitecore.Diagnostics.Log.Error("Attempted to raise the remote item events at the end of a bulk publish, but the event arguments were not valid.", this);
                return;
            }

            var eventArgs = sitecoreEventArgs.Parameters[0] as TargetDataCacheClearEventArgs;

            // Reflect to find expected private cache properties
            _prefetchCacheInfo = typeof(SqlDataProvider).GetProperty("PrefetchCache", BindingFlags.NonPublic | BindingFlags.Instance);
            if (_prefetchCacheInfo == null)
            {
                PublishingLog.Warn("Could not reflect to find the SqlDataProvider prefetch cache for clearing at the end of publish jobs.");
            }

            _languageCacheInfo = typeof(LanguageProvider).GetField("_languageCache", BindingFlags.NonPublic | BindingFlags.Instance);
            if (_languageCacheInfo == null)
            {
                PublishingLog.Warn("Could not reflect to find the Language cache for clearing at the end of publish jobs.");
            }

            var targetDb = _factory.GetDatabase(eventArgs.EventData.DatabaseName);
            if (targetDb == null) throw new ArgumentOutOfRangeException("targetDatabase", "No database name was specified.");

            try
            {
                ClearCaches(targetDb);
            }
            catch (Exception ex)
            {
                PublishingLog.Error("There was an error clearing all the target data caches at the end of the publish job", ex);
                throw;
            }
        }

        private void ClearCaches(Database database)
        {
            PublishingLog.Info("Starting to clear target data caches for database " + database.Name);

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
                    ((ICacheInfo)cache).Clear();
                }
            }

            // deviceItemsCache
            var cacheManager = ServiceLocator.ServiceProvider.GetRequiredService<BaseCacheManager>();

            // access result cache
            cacheManager.ClearAccessResultCache();

            // deviceItemsCache
            cacheManager.GetDeviceItemsCache().Clear();

            // PlaceholderCache
            this.ClearPlaceholderCaches(database.Name);

            // RuleCache
            var ruleFactory = ServiceLocator.ServiceProvider.GetRequiredService<BaseRuleFactory>();
            ruleFactory.InvalidateCache();

            // Language Provider
            if (_languageCacheInfo != null)
            {
                var languageCache = (ICacheInfo)_languageCacheInfo.GetValue(LanguageManager.Provider);
                languageCache.Clear();
            }

            // Proxy Provider
            var proxyManager = ServiceLocator.ServiceProvider.GetRequiredService<BaseProxyManager>();
            var proxyProviderFieldInfo = proxyManager.GetType().GetField("provider", BindingFlags.NonPublic | BindingFlags.Instance);
            if (proxyProviderFieldInfo != null)
            {
                var proxyProvider = (ProxyProvider)proxyProviderFieldInfo.GetValue(proxyManager);
                proxyProvider.ClearCaches();
            }

            // TemplateEngine
            database.Engines.TemplateEngine.Reset();

            // Translation
            _translate.ResetCache();

            PublishingLog.Info("Finished clearing target data caches for database " + database.Name);
        }

        private void ClearPlaceholderCaches(string databaseName)
        {
            var placeholderManager = ServiceLocator.ServiceProvider.GetRequiredService<BasePlaceholderCacheManager>();
            var cache = placeholderManager.GetPlaceholderCache(databaseName);
            var baseType = typeof(Sitecore.Caching.FieldRelatedItemCache);
            var lockField = baseType.GetField("Lock", BindingFlags.Static | BindingFlags.NonPublic);
            if (lockField != null)
            {
                var lockObject = lockField.GetValue(null);
                if (lockObject != null)
                {
                    lock (lockObject)
                    {
                        var itemIDsField = baseType.GetField("itemIDs", BindingFlags.Instance | BindingFlags.NonPublic);
                        if (itemIDsField != null)
                        {
                            var itemIDs = itemIDsField.GetValue(cache) as SafeDictionary<string, ID>;
                            if (itemIDs != null)
                            {
                                itemIDs.Clear();
                            }
                        }
                    }
                }
            }
        }
    }
}
